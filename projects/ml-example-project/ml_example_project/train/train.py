import logging
from datetime import datetime
from typing import Literal

import mlflow
import pytz
from constants.companies import get_company_by_code
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow.models import infer_signature
from pydantic import BaseModel
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from ml_example_project.train.configs import feature_lookup_config_list, get_company_train_configs
from ml_example_project.train.data import create_training_set
from ml_example_project.train.model import define_model_pipeline


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    is_run_on_databricks: bool = True
    is_use_feature_store: bool = False
    profile_name: str = "sylvia-liu"


def train_model(args: Args, spark: SparkSession) -> None:
    """Train model for specific company and time period.

    - Gets company specific configurations.
    - Creates training set from FeatureEngineeringClient or local dataframes.
    - Splits training set into training and validation sets.
    - Defines model pipeline and trains model.
    - Logs trained model, metrics, and signature with mlflow.

    Parameters:
        args (Args): Configuration arguments.
        spark (SparkSession): Spark session.
    """
    company_train_configs = get_company_train_configs(company_code=args.company)
    company_properties = get_company_by_code(company_code=args.company)

    # Get training data
    if (args.is_run_on_databricks) & (args.is_use_feature_store):
        fe = FeatureEngineeringClient()
    else:
        fe = None
        # Override it if set to True.
        # It can't be used unless you are running on Databricks
        args.is_use_feature_store = False

    company_id = company_properties.company_id
    training_set = create_training_set(
        spark=spark,
        company_id=company_id,
        feature_lookup_config_list=feature_lookup_config_list,
        company_train_configs=company_train_configs,
        fe=fe,
    )
    if args.is_use_feature_store:
        df_training = training_set.load_df().toPandas()  # type: ignore
    else:
        df_training = training_set.toPandas()  # type: ignore
    target = "recipe_difficulty_level_id"
    X_train, X_val, y_train, y_val = train_test_split(  # noqa
        df_training.drop(columns=[target]), df_training[target], test_size=0.2
    )

    if args.is_run_on_databricks:
        mlflow.set_tracking_uri("databricks")
    else:
        mlflow.set_tracking_uri(f"databricks://{args.profile_name}")

    mlflow.set_experiment("/Shared/ml_experiments/ml-example-project")
    timezone = pytz.timezone("UTC")
    timestamp_now = datetime.now(tz=timezone).strftime("%Y-%m-%d-%H:%M:%S")
    run_name = f"{args.company}_{timestamp_now}"

    with mlflow.start_run(run_name=run_name):
        pipeline = define_model_pipeline(task="classify", model_params=company_train_configs.model_params)
        pipeline.fit(X_train=X_train, y_train=y_train)  # type: ignore
        y_pred = pipeline.predict(model_input=X_val, context=None)  # type: ignore
        logging.info("Inferring signature...")
        accuracy = accuracy_score(y_true=y_val, y_pred=y_pred)
        mlflow.log_metric(key="classification_accuracy", value=accuracy)
        signature = infer_signature(model_input=X_train, model_output=y_train)
        mlflow.pyfunc.log_model(
            python_model=pipeline,
            artifact_path="test",
            registered_model_name=f"mloutputs.ml_example_project_{args.company}",
            signature=signature,
        )
