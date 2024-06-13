import pandas as pd
from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession

from dishes_forecasting.time_machine import get_forecast_year_weeks

spark = SparkSession.getActiveSession()


def download_weekly_variations(
    company: Company,
    prediction_date: str,
    num_weeks: int,
    env: str,
) -> DataFrame:
    company_id = company.company_id
    cut_off_day = company.cut_off_week_day

    df = get_forecast_year_weeks(
        prediction_date=pd.to_datetime(prediction_date),
        num_weeks=num_weeks,
        cut_off_day=cut_off_day,
    )

    max_yyyyww = (df["year"] * 100 + df["week"]).max()
    min_yyyyww = (df["year"] * 100 + df["week"]).min()

    df_weekly_dishes = (
        spark.read.table(f"{env}.mltesting.weekly_dish_variations")
        .select(
            "year",
            "week",
            "company_id",
            "variation_id",
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"year * 100 + week >= {min_yyyyww}")
        .filter(f"year * 100 + week <= {max_yyyyww}")
    )

    return df_weekly_dishes
