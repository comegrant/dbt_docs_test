import logging
from typing import Literal

import mlflow
from constants.companies import get_company_by_code
from pydantic import BaseModel, Field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from orders_forecasting.models.predict import MlflowConfig, get_predict_config
from orders_forecasting.predict.get_data import get_prediction_entities_for_week
from orders_forecasting.predict.predict_model import make_predictions
from orders_forecasting.spark_utils import set_primary_keys_table

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    prediction_date: str
    num_weeks: int = Field(gt=10, lt=21)
    target: Literal[
        "num_total_orders",
        "num_dishes_orders",
        "perc_dishes_orders",
    ]
    use_latest_model_run: bool = False


def run_predict_with_args(args: Args) -> DataFrame:
    # Get configurations and set mlflow
    company = get_company_by_code(args.company)
    predict_config = get_predict_config(
        env=args.env, target_col=args.target, company=company
    )
    mlflow_config = predict_config.mlflow_config
    model_config = predict_config.ml_model_config

    mlflow.set_tracking_uri(mlflow_config.mlflow_tracking_uri)
    mlflow.set_experiment(mlflow_config.experiment_tracking_dir)

    # Get prediction entities
    df = get_prediction_entities_for_week(
        company=company,
        prediction_date=args.prediction_date,
        num_weeks=int(args.num_weeks),
    )

    # Make prediction
    model_uri = model_config.model_uri
    if args.use_latest_model_run:
        logger.info("Using latest model run")
        model_uri = get_latest_model_run(
            mlflow_config=mlflow_config,
            env=args.env,
            company_code=args.company,
            target=args.target,
        )
    scores_df = make_predictions(model_uri=model_uri, df=df)

    # Save predictions
    save_predictions(
        scores_df=scores_df,
        env=args.env,
        prediction_date=args.prediction_date,
        target_col=args.target,
        model_uri=model_uri,
    )
    return scores_df


def get_latest_model_run(
    mlflow_config: MlflowConfig, env: str, company_code: str, target: str
) -> str:
    filter_string = (
        f"tags.company ILIKE '%{company_code}%' "
        + f"and tags.target_col ILIKE '%{target}%' "
        + f"and tags.env = '{env}' "
        + "and status = 'FINISHED' "
    )
    run_id = mlflow.search_runs(
        experiment_ids=[mlflow_config.experiment_id],
        order_by=["attribute.start_time DESC"],
        filter_string=filter_string,
    ).iloc[0]["run_id"]
    return f"runs:/{run_id}/orders_forecasting_model_{target}_{company_code}"


def save_predictions(
    scores_df: DataFrame,
    env: str,
    prediction_date: str,
    target_col: str,
    model_uri: str,
) -> None:
    # Create table if not exists
    table_name = f"{env}.mltesting.order_forecasting_{target_col}_predictions"
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                company_id VARCHAR(36),
                year INT,
                week INT,
                prediction_date DATE,
                prediction FLOAT,
                model_id STRING,
                model_name STRING,
                run_timestamp TIMESTAMP
            ) PARTITIONED BY (company_id, prediction_date)
        """
    )

    # Set primary keys columns to table
    set_primary_keys_table(
        full_table_name=table_name,
        primary_keys_cols=["company_id", "year", "week", "prediction_date"],
    )

    # Add prediction date as a column
    _, model_id, model_name = model_uri.split("/")
    scores_df = (
        scores_df.withColumn("prediction_date", f.to_date(f.lit(prediction_date)))
        .withColumn("model_id", f.lit(model_id))
        .withColumn("model_name", f.lit(model_name))
        .withColumn("run_timestamp", f.current_timestamp())
    )

    # Set spark configuration to overwrite only partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    scores_df.display()

    logger.info(scores_df.schema.names)

    # Overwrite table where partitions are the same
    scores_df.select(
        "company_id",
        "year",
        "week",
        "prediction_date",
        "prediction",
        "model_id",
        "model_name",
        "run_timestamp",
    ).write.mode("overwrite").insertInto(table_name)
