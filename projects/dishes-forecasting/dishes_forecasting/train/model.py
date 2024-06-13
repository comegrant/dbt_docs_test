from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
import pytz
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store.training_set import TrainingSet
from pycaret.regression import (
    blend_models,
    compare_models,
    finalize_model,
    predict_model,
    setup,
)

from dishes_forecasting.logger_config import logger

mlflow.set_tracking_uri("databricks")


def create_train_val_split(
    training_set: TrainingSet,
    num_holdout_weeks: int,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    traininng_set_pddf = training_set.load_df().toPandas()
    first_holdout_yyyyww = np.sort(
        (traininng_set_pddf["year"] * 100 + traininng_set_pddf["week"]).unique(),
    )[-num_holdout_weeks]

    df_train = traininng_set_pddf[
        (traininng_set_pddf["year"] * 100 + traininng_set_pddf["week"])
        < first_holdout_yyyyww
    ]

    df_val = traininng_set_pddf[
        (traininng_set_pddf["year"] * 100 + traininng_set_pddf["week"])
        >= first_holdout_yyyyww
    ]

    return df_train, df_val, first_holdout_yyyyww


def train_model(
    training_set: TrainingSet,
    model_config: dict,
    mlflow_config: dict,
    company_code: str,
    env: str,
    df_orders: pd.DataFrame | None = None,
) -> None:
    timezone = pytz.timezone("CET")
    timestamp_now = datetime.now(tz=timezone).strftime("%Y-%m-%d-%H:%M:%S")
    run_name = f"{company_code}_{timestamp_now}"
    # mlflow.set_experiment(experiment_name=f"{experiment_dir}/{experiment_name}")
    df_train, df_val, first_holdout_yyyyww = create_train_val_split(
        training_set=training_set,
        num_holdout_weeks=model_config["num_holdout_weeks"],
    )
    logger.info(f"Starting ML flow experiment, split point = {first_holdout_yyyyww}")
    with mlflow.start_run(run_name=run_name):
        experiment_setup = setup(
            data=df_train,
            test_data=df_val,
            target=model_config["target"],
            fold_strategy=model_config["fold_strategy"],
            data_split_shuffle=model_config["data_split_shuffle"],
            transform_target=model_config["transform_target"],
            transform_target_method=model_config["transform_target_method"],
            memory=False,
        )

        top_models = compare_models(
            n_select=model_config["n_select"],
            include=model_config["model_list"],
        )

        df_train_info = experiment_setup.pull()
        df_train_info.to_csv("./pycaret_training_info.csv", index=False)
        mlflow.log_artifact("pycaret_training_info.csv")

        blender = blend_models(top_models)
        df_holdout_with_pred = predict_model(blender)
        df_holdout_with_pred_to_log = df_holdout_with_pred[
            ["year", "week", "prediction_label"]
        ]
        df_holdout_with_pred_to_log.to_csv("./df_holdout.csv")
        if df_orders is not None:
            df_holdout_error, mae, mape = calculate_holdout_error(
                df_actual=df_orders,
                df_holdout_with_pred=df_holdout_with_pred,
            )
            df_holdout_error.to_csv("./df_holdout_error.csv")
            mlflow.log_artifact("./df_holdout_error.csv")
            mlflow.log_metric(key="holdout_mae", value=mae)
            mlflow.log_metric(key="holdout_mape", value=mape)
            df_holdout_error_agg = aggregate_holdout_errors(
                df_holdout_error=df_holdout_error,
            )
            df_holdout_error_agg.to_csv("./df_holdout_error_agg.csv")
            mlflow.log_artifact("./df_holdout_error_agg.csv")
        else:
            mlflow.log_artifact("df_holdout.csv")
        finalized_model = finalize_model(blender)
        fe = FeatureStoreClient()
        model_container_name = mlflow_config["model_container_name"]
        model_name = mlflow_config["registered_model_name"]
        registered_model_name = (
            f"{env}.{model_container_name}.{model_name}_{company_code}"
        )
        fe.log_model(
            model=finalized_model,
            artifact_path=mlflow_config["artifact_path"],
            flavor=mlflow.sklearn,
            training_set=training_set,
            registered_model_name=registered_model_name,
        )
        mlflow.end_run()


def calculate_holdout_error(
    df_holdout_with_pred: pd.DataFrame,
    df_actual: pd.DataFrame | None,
) -> tuple[pd.DataFrame, float, float]:
    df_holdout_error = df_actual.loc[df_holdout_with_pred.index, :]
    df_holdout_error["prediction_label"] = df_holdout_with_pred["prediction_label"]
    df_holdout_error["dish_quantity"] = (
        df_holdout_error["total_dish_quantity"] * df_holdout_error["dish_ratio"]
    ).round()

    df_holdout_error["dish_quantity_predicted"] = (
        df_holdout_error["total_dish_quantity"] * df_holdout_error["prediction_label"]
    ).round()

    df_holdout_error["error"] = (
        df_holdout_error["dish_quantity_predicted"] - df_holdout_error["dish_quantity"]
    )

    df_holdout_error["abs_error"] = df_holdout_error["error"].abs()
    df_holdout_error["error_pct"] = (
        df_holdout_error["error"] / df_holdout_error["dish_quantity"]
    )
    df_holdout_error["abs_error_pct"] = df_holdout_error["error_pct"].abs()
    mae = abs(df_holdout_error["error"]).mean()
    mape = abs(df_holdout_error["error_pct"]).mean()

    return df_holdout_error, mae, mape


def aggregate_holdout_errors(df_holdout_error: pd.DataFrame) -> pd.DataFrame:
    bins = [0, 10, 20, 50, 100, 100000]
    df_holdout_error_binned = df_holdout_error.groupby(
        pd.cut(df_holdout_error["dish_quantity"], bins=bins),
    ).agg({"abs_error": ["mean", "count"], "error_pct": ["mean"]})
    return df_holdout_error_binned
