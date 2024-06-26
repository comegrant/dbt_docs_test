import logging

import pandas as pd
from constants.companies import Company

from orders_forecasting.features.schemas import OrderHistoryFeatures
from orders_forecasting.inputs.helpers import (
    create_date_from_year_week,
    get_iso_week_numbers,
)
from orders_forecasting.models.features import FeaturesConfig
from orders_forecasting.paths import SQL_DIR
from orders_forecasting.utils import fetch_data_from_sql

from .time_helpers import add_moving_avg_features

logger = logging.getLogger(__name__)


def get_order_history_features(
    company: Company, features_config: FeaturesConfig
) -> pd.DataFrame:
    df_order_history = fetch_data_from_sql(
        sql_name="orders_weekly_aggregated",
        directory=SQL_DIR,
        catalog_name=features_config.source_catalog_name,
        catalog_scheme=features_config.source_catalog_schema,
        company_id=company.company_id,
    )

    # Clean raw data
    logger.info("Preliminarily cleaning order history df...")
    df_order_history = preliminary_clean_order_history_df(
        df_order_history=df_order_history,
    )

    df_order_history = get_seasonality_features(df_order_history=df_order_history)

    # Convert dtypes to same as pandara schema
    df_order_history = df_order_history.astype(
        {
            col: str(dtype)
            for col, dtype in OrderHistoryFeatures.to_schema().dtypes.items()
        }
    )

    return df_order_history.sort_values(by=["year", "week"])


def preliminary_clean_order_history_df(df_order_history: pd.DataFrame) -> pd.DataFrame:
    df_order_history = df_order_history.sort_values(by=["year", "week"])

    # may be some ancient test data if min_year is not set
    min_num_total_orders = 50
    df_order_history = df_order_history[
        df_order_history["num_total_orders"] >= min_num_total_orders
    ]
    df_order_history = ffill_missing_weeks(df_order_history=df_order_history)

    # The missed weeks have no "first date of week column"
    df_order_history = create_date_from_year_week(
        df=df_order_history,
        date_column_name="first_date_of_week",
    )
    return df_order_history


def ffill_missing_weeks(
    df_order_history: pd.DataFrame,
) -> pd.DataFrame:
    """Ffill missing week's total order

    Args:
        df_order_history (pd.DataFrame),
        colname: str

    Returns:
        pd.DataFrame
    """
    df_order_history = create_date_from_year_week(
        df=df_order_history,
        date_column_name="first_date_of_week",
    )
    start = df_order_history["first_date_of_week"].min()
    end = df_order_history["first_date_of_week"].max()

    logger.info(f"Filling missing weeks for start and end dates {start} and {end}")
    df_full_calendar = get_iso_week_numbers(start_date=start, end_date=end)
    # To eliminate the missing weeks, so that we have all weeks
    df_order_history = df_full_calendar.merge(
        df_order_history,
        on=["year", "week"],
        how="left",
    )
    df_order_history = df_order_history.drop_duplicates(subset=["year", "week"])

    df_order_history.loc[:, "company_id"] = df_order_history["company_id"].ffill()
    df_order_history.loc[:, "num_total_orders"] = df_order_history[
        "num_total_orders"
    ].ffill()

    return df_order_history


def get_seasonality_df(
    df_order_history: pd.DataFrame,
    target_col: str,
) -> pd.DataFrame:
    df = add_moving_avg_features(
        df=df_order_history, window_list=[52], origin_col=target_col
    )

    df["detrended"] = df[target_col] - df["moving_avg_52"]
    df["seasonality"] = df.groupby("week")["detrended"].transform("mean")
    df_seasonality = df[["week", "seasonality"]].drop_duplicates()
    df_seasonality = df_seasonality.reset_index().drop(columns="index")
    return df_seasonality


def get_seasonality_features(df_order_history: pd.DataFrame) -> pd.DataFrame:
    for target_col in ["num_total_orders"]:
        df_seasonality = get_seasonality_df(
            df_order_history=df_order_history, target_col=target_col
        )
        df = df_order_history.merge(df_seasonality, on="week")

    return df
