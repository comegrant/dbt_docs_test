import logging
from typing import Literal

from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from orders_forecasting.predict.get_data import get_predictions
from orders_forecasting.spark_utils import set_primary_keys_table

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    prediction_date: str


def run_generate_final_predictions_with_args(args: Args) -> None:
    company = get_company_by_code(args.company)

    # Get the last predictions for the company for the given prediction date
    df_predictions = get_predictions(
        env=args.env, company=company, prediction_date=args.prediction_date
    )

    # Calculate final predictions
    df_predictions = calculate_final_prediction(df_predictions=df_predictions)

    # Store prediction to prediction table with their model id
    save_predictions(
        df_predictions=df_predictions,
        env=args.env,
        prediction_date=args.prediction_date,
    )


def calculate_final_prediction(df_predictions: DataFrame) -> DataFrame:
    df_predictions = df_predictions.withColumn(
        "num_dishes_orders_calc",
        f.round(
            df_predictions["num_total_orders"] * df_predictions["perc_dishes_orders"]
        ),
    )
    df_predictions = df_predictions.withColumn(
        "num_dishes_orders_avg",
        f.round(
            (
                df_predictions["num_dishes_orders_calc"]
                + df_predictions["num_dishes_orders"]
            )
            / 2
        ),
    )
    df_predictions = df_predictions.withColumn(
        "num_dishes_orders", f.round(df_predictions["num_dishes_orders"])
    ).withColumn("num_total_orders", f.round(df_predictions["num_total_orders"]))
    return df_predictions


def save_predictions(
    df_predictions: DataFrame,
    env: str,
    prediction_date: str,
) -> None:
    # Create table if not exists
    table_name = f"{env}.mltesting.order_forecasting_predictions"
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                company_id VARCHAR(36),
                year INT,
                week INT,
                prediction_date DATE,
                num_total_orders INT,
                num_dishes_orders_calc INT,
                num_dishes_orders_avg INT,
                num_dishes_orders INT,
                perc_dishes_orders FLOAT
            ) PARTITIONED BY (company_id, prediction_date)
        """
    )

    # Set primary keys columns to table
    set_primary_keys_table(
        full_table_name=table_name,
        primary_keys_cols=["company_id", "year", "week", "prediction_date"],
    )

    # Add prediction date as a column
    df_predictions = df_predictions.withColumn(
        "prediction_date", f.to_date(f.lit(prediction_date))
    )

    # Overwrite only partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Save predictions
    df_predictions.select(
        "company_id",
        "year",
        "week",
        "prediction_date",
        "num_total_orders",
        "num_dishes_orders_calc",
        "num_dishes_orders_avg",
        "num_dishes_orders",
        "perc_dishes_orders",
    ).write.mode("overwrite").insertInto(table_name)
