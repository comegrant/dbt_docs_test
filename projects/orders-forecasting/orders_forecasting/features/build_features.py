import logging

import numpy as np
import pandas as pd
from constants.companies import Company

from orders_forecasting.features.estimations import get_estimations_features
from orders_forecasting.features.holiday_features import get_holiday_features
from orders_forecasting.features.order_history import get_order_history_features
from orders_forecasting.features.time_helpers import (
    add_lag_features,
    add_moving_avg_features,
)
from orders_forecasting.models.company import CompanyConfig
from orders_forecasting.models.features import FeaturesConfig

logger = logging.getLogger(__name__)


def get_features(
    company: Company,
    company_config: CompanyConfig,
    features_config: FeaturesConfig,
) -> pd.DataFrame:
    df_order_history = get_order_history_features(
        company=company,
        features_config=features_config,
    )
    logger.info(
        df_order_history.duplicated(subset=["year", "week", "company_id"]).sum()
    )

    # For weeks further ahead than the prediction date, we use the estimated values
    df_estimations = get_estimations_features(
        company=company,
        company_config=company_config,
        features_config=features_config,
    )
    logger.info(df_estimations.duplicated(subset=["year", "week", "company_id"]).sum())

    df = df_order_history.merge(
        df_estimations, on=["year", "week", "company_id"], how="left"
    )

    logger.info(df.duplicated(subset=["year", "week", "company_id"]).sum())

    # Remove the irregular days
    df = df[df["num_days_to_cut_off"] >= 0]

    df = get_retention_features(df)  # Depends on both order history and estimations

    # Compute lag features from today and 52 weeks forwards, shouldn't stop at today
    df = generage_lag_and_moving_avg_feats(
        df, df_estimations, df_order_history
    )  # Only use 52 weeks lag

    logger.info(df.duplicated(subset=["year", "week", "company_id"]).sum())

    logger.info("Adding feature: holiday calendar..")
    df_holiday_calendar = get_holiday_features(
        country=company.country,
        year_min=df["year"].min(),
        year_max=(df["year"].max() + 1),
    )
    df = df.merge(df_holiday_calendar, on=["year", "week"], how="left")

    df = df.sort_values(by="estimation_date")

    df["country"].fillna(company.country, inplace=True)
    df.loc[:, df_holiday_calendar.columns] = df[df_holiday_calendar.columns].fillna(0)

    df = df.astype({"year": "int32", "week": "int32"})

    return df, df_order_history, df_estimations, df_holiday_calendar


def get_retention_features(df: pd.DataFrame) -> pd.DataFrame:
    for target_col in [
        "num_total_orders",
        "num_mealbox_orders",
        "num_dishes_orders",
        "perc_dishes_orders",
    ]:
        df_retention_total = get_retention_df(
            df_history=df,
            estimation_col="quantity_estimated_total",
            target_col=target_col,
            is_group_by_week=False,
        )

        df_retention_total = df_retention_total.rename(
            columns={
                f"{target_col}_retention_rate": f"{target_col}_retention_rate_total"
            },
        )

        df = df.merge(df_retention_total, on="num_days_to_cut_off", how="left")

        df_retention_dishes = get_retention_df(
            df_history=df.dropna(),
            estimation_col="quantity_estimated_dishes",
            is_group_by_week=False,
            target_col=target_col,
        )

        df_retention_dishes = df_retention_dishes.rename(
            columns={
                f"{target_col}_retention_rate": f"{target_col}_retention_rate_dishes"
            },
        )

        df = df.merge(df_retention_dishes, on=["num_days_to_cut_off"])
        df[f"{target_col}_retention_projection_total"] = (
            df[f"{target_col}_retention_rate_total"] * df["quantity_estimated_total"]
        )
        df[f"{target_col}_retention_projection_dishes"] = (
            df[f"{target_col}_retention_rate_dishes"] * df["quantity_estimated_dishes"]
        )

    return df


def get_retention_df(
    df_history: pd.DataFrame,
    estimation_col: str,
    target_col: str,
    is_group_by_week: bool,
) -> pd.DataFrame:
    """retention refers to the ratio between actual order / estimation
        aggregated over the number of days to cut off, take median

    Args:
        df_history (pd.DataFrame): historical data. Must contain
         - num_days_to_cut_off
         - estimation_col
         - target_col
        estimation_col (str): estimated quantity column name
        target_col (str): target's column name

    Returns:
        pd.DataFrame: df that contains columns
         - num_days_to_cut_off
         - retention_rate
    """
    df_history.loc[:, f"{target_col}_retention_rate"] = 0.0
    df_history.loc[df_history[estimation_col] > 0, f"{target_col}_retention_rate"] = (
        df_history[target_col] / df_history[estimation_col]
    )
    if is_group_by_week:
        df_retention_rate = df_history.groupby(["num_days_to_cut_off", "week"])[
            f"{target_col}_retention_rate"
        ].median()
        df_retention_rate = pd.DataFrame(df_retention_rate).reset_index()

        # Need to fill the missing weeks
        min_num_days_to_cutoff = df_retention_rate["num_days_to_cut_off"].min()
        max_num_days_to_cutoff = df_retention_rate["num_days_to_cut_off"].max()
        unique_num_days = np.arange(
            min_num_days_to_cutoff,
            (max_num_days_to_cutoff + 1),
        )
        unique_weeks = df_retention_rate["week"].unique()

        df_retention_full = pd.DataFrame({"week": unique_weeks})
        df_retention_full = df_retention_full.merge(
            pd.DataFrame({"num_days_to_cut_off": unique_num_days}),
            how="cross",
        )
        df_retention_full = df_retention_full.merge(df_retention_rate, how="left")
        df_retention_full[f"{target_col}_retention_rate"] = df_retention_full[
            f"{target_col}_retention_rate"
        ].fillna(
            method="ffill",
        )
    else:
        df_retention_rate = df_history.groupby(["num_days_to_cut_off"])[
            f"{target_col}_retention_rate"
        ].median()
        df_retention_full = pd.DataFrame(df_retention_rate).reset_index()

    df_history = df_history.drop(columns=f"{target_col}_retention_rate")

    # return df_retention_rate
    return df_retention_full


def generage_lag_and_moving_avg_feats(
    df: pd.DataFrame,
    df_estimations: pd.DataFrame,
    df_order_history: pd.DataFrame,
) -> pd.DataFrame:
    # First, construct a dataframe that contains all weeks
    # Since future weeks are not available in order history
    # But lags should be available for them
    df_unique_weeks = df_estimations[["year", "week"]].drop_duplicates()
    max_available_weeks = (
        df_order_history["year"] * 100 + df_order_history["week"]
    ).max()
    df_missing_weeks = df_unique_weeks[
        (df_unique_weeks["year"] * 100 + df_unique_weeks["week"]) > max_available_weeks
    ].copy()
    # Append the missing columns so that they can be concat into order history
    for column in df_order_history.columns:
        if column not in df_missing_weeks.columns:
            df_missing_weeks.loc[:, column] = None
    df_full_weeks = pd.concat([df_order_history, df_missing_weeks], ignore_index=True)

    df_full_weeks = df_full_weeks.sort_values(by=["year", "week"])

    logger.info("Add moving avg and lag to target")
    df = generate_lag_moving_avg_features(
        df_full_weeks=df_full_weeks, df=df, target_col="num_total_orders"
    )

    return df


def generate_lag_moving_avg_features(
    df_full_weeks: pd.DataFrame,
    df: pd.DataFrame,
    target_col: str,
) -> pd.DataFrame:
    logger.info("Adding feature: moving average..")
    df_moving_avg = add_moving_avg_features(
        df=df_full_weeks,
        origin_col=target_col,
        window_list=[12, 52],
    )

    logger.info("Adding feature: lags..")
    df_lags = add_lag_features(
        df=df_full_weeks,
        origin_col=target_col,
        lag_list=[12, 52],
    )
    df = df.merge(
        df_moving_avg[["year", "week", "moving_avg_12", "moving_avg_52"]],
        on=["year", "week"],
        how="left",
    )

    df = df.merge(
        df_lags[["year", "week", "lag_12", "lag_52"]],
        on=["year", "week"],
        how="left",
    )

    return df
