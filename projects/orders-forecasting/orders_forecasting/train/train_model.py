import logging
from datetime import datetime, timezone

import mlflow
import mlflow.experiments
import pandas as pd
from constants.companies import Company
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.training_set import TrainingSet
from pycaret.regression import setup

from orders_forecasting.evaluate.metrics import get_metrics
from orders_forecasting.models.company import CompanyConfig
from orders_forecasting.models.mlflow import MlflowConfig, get_mlflow_config
from orders_forecasting.models.train import TrainConfig
from orders_forecasting.paths import PROJECT_DIR
from orders_forecasting.train.make_dataset import create_train_val_split

logger = logging.getLogger(__name__)


def train_ensemble_model(
    company: Company,
    training_set: TrainingSet,
    target_col: str,
    env: str,
    train_config: TrainConfig,
    company_config: CompanyConfig,
    is_finalize: bool = False,
) -> tuple:
    logger.info("Training model with MLflow")
    # Generate dataset, train/test/validate split
    mlflow_config = get_mlflow_config(env=env)
    ml_config = train_config.ml_config

    df_train, df_holdout = create_train_val_split(
        training_set=training_set,
        train_start_yyyyww=company_config.train_start[target_col],
        num_holdout_weeks=ml_config.num_holdout_weeks,
    )

    max_yyyy_ww_train = (df_train["year"] * 100 + df_train["week"]).max()
    time_now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    run_name = f"{company.company_code}_{target_col}_train_{max_yyyy_ww_train}_{time_now_str}"

    mlflow.set_tracking_uri(mlflow_config.mlflow_tracking_uri)
    mlflow.set_experiment(mlflow_config.experiment_tracking_dir)

    logger.info(f"Starting ML flow experiment, split point = {max_yyyy_ww_train}")
    with mlflow.start_run(run_name=run_name):
        mlflow.set_tag("run_datetime", time_now_str)

        # Set up experiment
        experiment_reg = setup(
            data=df_train,
            target=target_col,
            test_data=df_holdout,
            fold_strategy=ml_config.fold_strategy,
            data_split_shuffle=ml_config.data_split_shuffle,
            memory=False,
            html=False,
            numeric_features=train_config.train_features.numeric_features,
            categorical_features=train_config.train_features.categorical_features,
        )

        # Train model
        logger.info(train_config)
        best_models = experiment_reg.compare_models(
            n_select=train_config.n_select,
            include=train_config.ml_model_list,
        )
        # To be logged
        df_train_info = experiment_reg.pull()

        top_models_blend = experiment_reg.blend_models(estimator_list=best_models)

        df_train_artifact_path = f"{PROJECT_DIR}/df_train.csv"
        df_train.to_csv(df_train_artifact_path, index=False)
        df_train_info_artifact_path = f"{PROJECT_DIR}/training_info.csv"
        df_train_info.to_csv(df_train_info_artifact_path, index=False)

        log_metrics(
            experiment_reg=experiment_reg,
            df_holdout=df_holdout,
            model=top_models_blend,
            target_col=target_col,
        )
        mlflow.log_artifact(
            df_train_artifact_path,
            artifact_path="training_data",
        )
        mlflow.log_artifact(
            df_train_info_artifact_path,
            artifact_path="training_info",
        )

        if is_finalize:
            final_model = experiment_reg.finalize_model(top_models_blend)
        else:
            final_model = top_models_blend

        save_model(
            mlflow_config=mlflow_config,
            env=env,
            target_col=target_col,
            company=company.company_code,
            training_set=training_set,
            finalized_model=final_model,
        )

        mlflow.end_run()


def save_model(
    mlflow_config: MlflowConfig,
    env: str,
    target_col: str,
    company: str,
    training_set: TrainingSet,
    finalized_model: object,
) -> None:
    fe = FeatureEngineeringClient()
    mlmodel_container_name = mlflow_config.mlmodel_container_name
    model_name = mlflow_config.registered_model_name
    registered_model_name = f"{env}.{mlmodel_container_name}.{model_name}_{target_col}_{company}"
    fe.log_model(
        model=finalized_model,
        artifact_path=f"{mlflow_config.artifact_path}_{target_col}_{company}",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=registered_model_name,
    )
    mlflow.set_tag("env", env)
    mlflow.set_tag("registered_model_name", registered_model_name)
    mlflow.set_tag("target_col", target_col)
    mlflow.set_tag("company", company)


def log_metrics(experiment_reg: object, df_holdout: pd.DataFrame, model: object, target_col: str) -> None:
    actual = df_holdout.pop(target_col)
    prediction = experiment_reg.predict_model(model)

    metric_dict = get_metrics(df_actual=actual, df_pred=prediction["prediction_label"])

    mlflow.log_metric("holdout_mape", metric_dict["mape"])
    mlflow.log_metric("holdout_mae", metric_dict["mae"])
