import logging
from typing import Optional

import pandas as pd
import streamlit as st
from catalog_connector import connection

logger = logging.getLogger(__name__)


def get_order_history(company_id: str) -> pd.DataFrame:
    """
    Loads the needed data for the workflow.
    """
    df_order_history = connection.sql(
        f"""
            select
                menu_year,
                menu_week,
                total_orders,
                total_orders_with_flex,
                flex_share
            from prod.mlgold.tofu_order_history
            where company_id = '{company_id}'
            and menu_year >= 2021
        """
    ).toPandas()

    return df_order_history


def get_calendar() -> pd.DataFrame:
    """
    Loads the needed data for the workflow.
    """
    df_calendar = connection.sql(
        """
            select distinct
                extract(yearofweek from monday_date) as menu_year,
                extract(week from monday_date) as menu_week,
                monday_date
            from prod.gold.dim_dates
            where
                financial_year >= 2021
                and day_of_week = 1
            order by monday_date
        """
    ).toPandas()

    return df_calendar


def get_latest_forecasts(company_id: str) -> pd.DataFrame:
    """
    Loads the needed data for the workflow.
    """
    df_latest_forecasts = connection.sql(
        f"""
            select
                *
            from prod.mlgold.tofu_latest_forecasts
            where company_id = '{company_id}'
            and menu_year >= 2021
        """
    ).toPandas()

    return df_latest_forecasts


@st.cache_data
def get_forecast_start_from_db(
    company_id: str,
) -> tuple[int, int]:
    df_forecast_start = connection.sql(
        f"""
            select
                extract(year from date_add(menu_week_monday_date, 7)) as forecast_start_year,
                extract(week from date_add(menu_week_monday_date, 7)) as forecast_start_week
            from prod.intermediate.int_latest_menu_week_passed_cutoff
            where company_id = '{company_id}'
        """
    ).toPandas()
    forecast_start_year, forecast_start_week = df_forecast_start.head(1).values[0]
    return forecast_start_year, forecast_start_week


def append_pandas_df_to_catalog(
    df: pd.DataFrame,
    table_name: str,
    schema: Optional[str] = "forecasting",
) -> None:
    spark_df = connection.spark().createDataFrame(df)
    connection.table(f"{schema}.{table_name}").append(spark_df)
