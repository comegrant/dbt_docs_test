from datetime import datetime
from typing import Optional, Union

import mlflow
import numpy as np
import pandas as pd
import pytz
from constants.companies import Company
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from databricks.feature_store.training_set import TrainingSet
from dishes_forecasting.train.metrics import get_test_metrics
from dishes_forecasting.train.model import define_ensemble_model
from dishes_forecasting.schema import feature_schema


def train_model(
    training_set: Union[pd.DataFrame, TrainingSet],
    params_lgb: dict,
    params_xgb: dict,
    params_rf: dict,
    company: Company,
    env: str,
    spark: SparkSession,
    train_config: dict,
    is_running_on_databricks: Optional[bool] = False,
    is_register_model: Optional[bool] = False,
    is_log_model: Optional[bool] = True,
    profile_name: Optional[str] = "sylvia-liu"  # For databricks connect authentication
) -> tuple[Pipeline, pd.Series, pd.Series, pd.Series, pd.Series]:
    company_code = company.company_code
    company_id = company.company_id
    timezone = pytz.timezone("CET")
    timestamp_now = datetime.now(tz=timezone).strftime("%Y-%m-%d-%H:%M:%S")
    run_name = f"{company_code}_{timestamp_now}"
    if is_running_on_databricks:
        mlflow.set_tracking_uri("databricks")
    else:
        mlflow.set_tracking_uri(f"databricks://{profile_name}")
    mlflow.set_experiment("/Shared/ml_experiments/dishes-forecasting")
    with mlflow.start_run(run_name=run_name):
        # Define the numeric and categorical features

        # Create final pipeline with preprocessor and ensemble
        custom_pipeline = define_ensemble_model(
            company_code=company_code,
            params_lgb=params_lgb,
            params_xgb=params_xgb,
            params_rf=params_rf
        )
        if not isinstance(training_set, pd.DataFrame):
            training_set_df = training_set.load_df().toPandas()
        else:
            training_set_df = training_set
        training_set_df = training_set_df.dropna()
        # Coercing datatype
        print(f"Forcing feature columns to be the defined data type...") # noqa
        feature_schema.coerce = True
        training_set_df = feature_schema.validate(training_set_df)

        X_train, X_test, y_train, y_test = split_train_test(  # noqa
            training_set=training_set_df,
            method="sequential",
            random_state=42,
            test_size=0.02,
        )
        # Print the shapes of the resulting datasets
        print(f"Training set shape: {X_train.shape}") # noqa
        print(f"Test set shape: {X_test.shape}") # noqa

        # Fit the custom pipeline
        custom_pipeline.fit(X_train, np.log(y_train))
        y_pred = custom_pipeline.predict(X_test)
        y_pred_transformed = np.exp(y_pred)

        df_test_metrics, mae, mape, df_test_binned = get_test_metrics(
            spark=spark,
            env=env,
            X_test=X_test,
            y_pred_transformed=y_pred_transformed,
            min_yyyyww=train_config["train_start_yyyyww"],
            max_yyyyww=train_config["train_end_yyyyww"],
            company_id=company_id,
            is_normalized=False
        )
        mlflow.log_metric(key="mape", value=mape)
        mlflow.log_metric(key="mae", value=mae)
        for k, v in params_lgb.items():
            mlflow.log_params({"lgb_" + k: float(v)})
        for k, v in params_xgb.items():
            mlflow.log_params({"xgb_" + k: float(v)})
        for k, v in params_rf.items():
            mlflow.log_params({"rf_" + k: float(v)})
        # Log df_test_binned as an artifact
        # Flatten the columns in df_test_binned
        df_test_binned.columns = [
            '_'.join(col).strip() for col in df_test_binned.columns.values
        ]
        df_test_binned.to_csv("test_binned_results.csv", index=True)
        mlflow.log_artifact("test_binned_results.csv", "test_binned_results.csv")
        df_test_metrics.to_csv("test_metrics.csv", index=True)
        mlflow.log_artifact("test_metrics.csv", "test_metrics.csv")
        X_test.to_csv("xtest.csv", index=False)
        mlflow.log_artifact("xtest.csv", "xtest.csv")

        model_container_name = "mloutputs"
        model_name = f"dishes_forecasting_{company_code}"
        registered_model_name = (
            f"{env}.{model_container_name}.{model_name}"
        )
        if is_register_model:
            mlflow.sklearn.log_model(
                sk_model=custom_pipeline,
                artifact_path=model_name,
                input_example=X_train,
                registered_model_name=registered_model_name,
            )
        elif is_log_model:
            mlflow.sklearn.log_model(
                sk_model=custom_pipeline,
                artifact_path=model_name,
                input_example=X_train,
            )
        mlflow.end_run()

    return custom_pipeline, X_train, X_test, y_train, y_test, mape, mae, df_test_metrics, df_test_binned


def split_train_test(
    training_set: pd.DataFrame,
    method: Optional[str] = "random",
    random_state: Optional[int] = 42,
    test_size: Optional[float] = 0.2,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    X = training_set.drop(  # noqa
        ['variation_ratio'],
        errors="ignore",
        axis=1
    )
    y = training_set['variation_ratio']
    if method == "random":
        X_train, X_test, y_train, y_test = train_test_split( # noqa
            X, y, test_size=test_size, random_state=random_state
        )
    elif method == "sequential":
        num_rows = X.shape[0]
        num_test_rows = round(num_rows * test_size)
        X_train, X_test, y_train, y_test = ( # noqa
            X[:-num_test_rows],
            X[-num_test_rows:],
            y[:-num_test_rows],
            y[-num_test_rows:],
        )

    return X_train, X_test, y_train, y_test
