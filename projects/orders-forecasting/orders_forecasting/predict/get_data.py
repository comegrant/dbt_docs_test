import logging

import pandas as pd
from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from orders_forecasting.time_machine import get_forecast_year_weeks

spark = SparkSession.getActiveSession()
logger = logging.getLogger(__name__)


def get_prediction_entities_for_week(
    company: Company, prediction_date: str, num_weeks: int
) -> DataFrame:
    company_id = company.company_id
    cut_off_day = company.cut_off_week_day

    df = get_forecast_year_weeks(
        prediction_date=pd.to_datetime(prediction_date),
        num_weeks=num_weeks,
        cut_off_day=cut_off_day,
    )

    df = df.withColumn("company_id", f.lit(company_id)).withColumn(
        "estimation_date", f.to_timestamp(f.lit(prediction_date))
    )

    return df


def get_predictions(env: str, company: Company, prediction_date: str) -> DataFrame:
    df_num_total_orders_preds = get_predictions_for_target(
        target="num_total_orders",
        env=env,
        company=company,
        prediction_date=prediction_date,
    )
    df_num_dishes_orders_preds = get_predictions_for_target(
        target="num_dishes_orders",
        env=env,
        company=company,
        prediction_date=prediction_date,
    )
    df_perc_dishes_orders_preds = get_predictions_for_target(
        target="perc_dishes_orders",
        env=env,
        company=company,
        prediction_date=prediction_date,
    )

    return df_num_total_orders_preds.join(
        df_num_dishes_orders_preds, on=["company_id", "year", "week"], how="outer"
    ).join(df_perc_dishes_orders_preds, on=["company_id", "year", "week"], how="outer")


def get_predictions_for_target(
    target: str, env: str, company: Company, prediction_date: str
) -> DataFrame:
    return (
        spark.table(f"{env}.mltesting.order_forecasting_{target}_predictions")
        .filter(f"company_id = '{company.company_id}'")
        .filter(f"prediction_date = '{prediction_date}'")
        .withColumnRenamed("prediction", target)
        .select("company_id", "year", "week", target)
    )
