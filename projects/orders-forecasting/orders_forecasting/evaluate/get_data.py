import logging

from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


def get_actual_values_for_yearweek(
    env: str, company: Company, year: int, week: int
) -> DataFrame:
    return (
        spark.table(f"{env}.mltesting.orders_weekly_aggregated")
        .filter(f"company_id = '{company.company_id}'")
        .filter(f"year = '{year}'")
        .filter(f"week = '{week}'")
        .withColumnRenamed("num_total_orders", "actual_num_total_orders")
        .withColumnRenamed("num_dishes_orders", "actual_num_dishes_orders")
        .withColumnRenamed("perc_dishes_orders", "actual_perc_dishes_orders")
    )


def get_predictions_for_yearweek(
    env: str, company: Company, year: int, week: int
) -> DataFrame:
    return (
        spark.table(f"{env}.mltesting.order_forecasting_predictions")
        .filter(f"company_id = '{company.company_id}'")
        .filter(f"year = '{year}'")
        .filter(f"week = '{week}'")
        .withColumnRenamed("num_total_orders", "pred_num_total_orders")
        .withColumnRenamed("num_dishes_orders", "pred_num_dishes_orders")
        .withColumnRenamed("num_dishes_orders_calc", "pred_num_dishes_orders_calc")
        .withColumnRenamed("num_dishes_orders_avg", "pred_num_dishes_orders_avg")
        .withColumnRenamed("perc_dishes_orders", "pred_perc_dishes_orders")
    )
