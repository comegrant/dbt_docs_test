import logging
from typing import Literal

from constants.companies import get_company_by_code
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from orders_forecasting.evaluate.get_data import (
    get_actual_values_for_yearweek,
    get_predictions_for_yearweek,
)
from orders_forecasting.evaluate.metrics import evaluate_predictions

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    year: int
    week: int


def evaluate_with_args(args: Args) -> DataFrame:
    # Get prediction entities
    company = get_company_by_code(args.company)

    # Evaluate prediction.
    # We have different predictions based on days before cutoff.
    # So all predictions with their respective prediction date need to be evaluated

    # We only get predictions were we already have the true values for that week year that was predicted

    # Get actual values that are not already in the evaluation table
    df_actual_values = get_actual_values_for_yearweek(
        env=args.env,
        company=company,
        year=args.year,
        week=args.week,
    )

    # Get predictions for the orders not already in the evaluation table
    df_predictions = get_predictions_for_yearweek(
        env=args.env,
        company=company,
        year=args.year,
        week=args.week,
    )

    # Evaluate predictions
    df_evaluation, dct_metrics = evaluate_predictions(
        df_actual_values=df_actual_values,
        df_predictions=df_predictions,
    )

    logger.info(f"Metrics: {df_evaluation.show()}")

    # Save evaluation
    save_evaluation(
        df_evaluation=df_evaluation,
        env=args.env,
    )

    logger.info(dct_metrics)


def save_evaluation(df_evaluation: DataFrame, env: str) -> None:
    table_name = f"{env}.mltesting.order_forecasting_evaluation"
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                company_id VARCHAR(36),
                year INT,
                week INT,
                prediction_date DATE,
                actual_num_total_orders FLOAT,
                pred_num_total_orders FLOAT,
                actual_num_dishes_orders FLOAT,
                pred_num_dishes_orders_calc FLOAT,
                pred_num_dishes_orders_avg FLOAT,
                pred_num_dishes_orders FLOAT,
                actual_perc_dishes_orders FLOAT,
                pred_perc_dishes_orders FLOAT,
                num_total_orders_error INT,
                num_total_orders_abs_error INT,
                num_total_orders_abs_error_pct FLOAT,
                num_total_orders_error_pct FLOAT,
                num_dishes_orders_error INT,
                num_dishes_orders_abs_error INT,
                num_dishes_orders_abs_error_pct FLOAT,
                num_dishes_orders_error_pct FLOAT,
                num_dishes_orders_calc_error INT,
                num_dishes_orders_calc_abs_error INT,
                num_dishes_orders_calc_abs_error_pct FLOAT,
                num_dishes_orders_calc_error_pct FLOAT,
                num_dishes_orders_avg_error INT,
                num_dishes_orders_avg_abs_error INT,
                num_dishes_orders_avg_abs_error_pct FLOAT,
                num_dishes_orders_avg_error_pct FLOAT,
                perc_dishes_orders_error FLOAT,
                perc_dishes_orders_abs_error FLOAT,
                perc_dishes_orders_abs_error_pct FLOAT,
                perc_dishes_orders_error_pct FLOAT
            ) PARTITIONED BY (company_id, year, week, prediction_date)
        """
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_evaluation.select(
        "company_id",
        "year",
        "week",
        "prediction_date",
        "actual_num_total_orders",
        "pred_num_total_orders",
        "actual_num_dishes_orders",
        "pred_num_dishes_orders_calc",
        "pred_num_dishes_orders_avg",
        "pred_num_dishes_orders",
        "actual_perc_dishes_orders",
        "pred_perc_dishes_orders",
        "num_total_orders_error",
        "num_total_orders_abs_error",
        "num_total_orders_abs_error_pct",
        "num_total_orders_error_pct",
        "num_dishes_orders_error",
        "num_dishes_orders_abs_error",
        "num_dishes_orders_abs_error_pct",
        "num_dishes_orders_error_pct",
        "num_dishes_orders_calc_error",
        "num_dishes_orders_calc_abs_error",
        "num_dishes_orders_calc_abs_error_pct",
        "num_dishes_orders_calc_error_pct",
        "num_dishes_orders_avg_error",
        "num_dishes_orders_avg_abs_error",
        "num_dishes_orders_avg_abs_error_pct",
        "num_dishes_orders_avg_error_pct",
        "perc_dishes_orders_error",
        "perc_dishes_orders_abs_error",
        "perc_dishes_orders_abs_error_pct",
        "perc_dishes_orders_error_pct",
    ).write.mode("overwrite").insertInto(
        f"{env}.mltesting.order_forecasting_evaluation"
    )
