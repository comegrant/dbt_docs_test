import logging
from datetime import date

import mlflow
from constants.companies import get_company_by_code
from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel, Field
from pydantic_argparser import parser_for
from pydantic_argparser.parser import decode_args
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from customer_churn.models.mlflow import MlflowConfig, get_mlflow_config
from customer_churn.predict.predict_model import predict_customer_churn_for_date
from customer_churn.spark_utils import set_primary_keys_table

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


class RunArgs(BaseModel):
    company: str = Field("RN")
    prediction_date: date = Field(date.today())
    env: str = Field("dev")


def run_with_args(args: RunArgs) -> None:
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    # Load environment variables
    load_dotenv(find_dotenv())
    mlflow_config = get_mlflow_config(env=args.env)
    mlflow.set_tracking_uri(mlflow_config.mlflow_tracking_uri)

    logger.info(f"Running predict with args {args}")

    # Get features
    company = get_company_by_code(args.company)

    # Preprocess features for prediction
    logger.info("Preprocessing features for prediction...")

    # Get latest model run
    model_uri = get_latest_model_run(
        mlflow_config=mlflow_config,
        env=args.env,
        company_code=company.company_code,
    )

    # Make predictions
    predictions = predict_customer_churn_for_date(
        env=args.env,
        company=company,
        prediction_date=args.prediction_date,
        model_uri=model_uri,
    )

    logger.info(predictions.first().asDict())

    save_predictions(
        scores_df=predictions,
        env=args.env,
        prediction_date=args.prediction_date,
        model_uri=model_uri,
    )

    logger.info(predictions)

    return predictions


def get_latest_model_run(
    mlflow_config: MlflowConfig, env: str, company_code: str
) -> str:
    filter_string = (
        f"tags.company ILIKE '%{company_code}%' " + f"and tags.env = '{env}' "
    )
    run_id = mlflow.search_runs(
        experiment_ids=[mlflow_config.experiment_id],
        order_by=["attribute.start_time DESC"],
        filter_string=filter_string,
    )

    if len(run_id) == 0:
        logger.info(run_id)
        raise ValueError(f"No model run found for {env} {company_code}")

    run_id = run_id.iloc[0]["run_id"]
    return f"runs:/{run_id}/customer_churn_model_{company_code}"


def save_predictions(
    scores_df: DataFrame,
    env: str,
    prediction_date: str,
    model_uri: str,
) -> None:
    # Create table if not exists
    table_name = f"{env}.mltesting.customer_churn_predictions"
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                company_id VARCHAR(36),
                prediction_date DATE,
                prediction STRING,
                model_id STRING,
                model_name STRING,
                run_timestamp TIMESTAMP
            ) PARTITIONED BY (company_id, prediction_date)
        """
    )

    # Set primary keys columns to table
    set_primary_keys_table(
        full_table_name=table_name,
        primary_keys_cols=["company_id", "prediction_date"],
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
        "prediction_date",
        "prediction",
        "model_id",
        "model_name",
        "run_timestamp",
    ).write.mode("overwrite").insertInto(table_name)


def run() -> None:
    args = parser_for(RunArgs).parse_args()

    run_with_args(
        decode_args(args, RunArgs),
    )


if __name__ == "__main__":
    run()
