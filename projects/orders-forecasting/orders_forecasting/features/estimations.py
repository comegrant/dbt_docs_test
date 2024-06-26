import logging
from datetime import datetime

import pandas as pd
import pandera as pa
from constants.companies import Company

from orders_forecasting.features.schemas import OrderEstimationFeatures
from orders_forecasting.inputs.helpers import (
    create_future_df,
    get_cut_off_date,
    get_forecast_start,
    is_year_has_week_53,
)
from orders_forecasting.models.company import CompanyConfig
from orders_forecasting.models.features import FeaturesConfig
from orders_forecasting.paths import SQL_DIR
from orders_forecasting.utils import fetch_data_from_sql

logger = logging.getLogger(__name__)


def get_estimations_features(
    company: Company,
    company_config: CompanyConfig,
    features_config: FeaturesConfig,
) -> pd.DataFrame:
    # Get raw data from catalog tables
    df_estimations_total, df_estimations_dishes = get_estimations_from_catalog(
        catalog_name=features_config.source_catalog_name,
        catalog_schema=features_config.source_catalog_schema,
        company=company,
    )

    # Clean raw data
    df_estimations = preliminarily_clean_data(
        df_estimations_total, df_estimations_dishes
    )

    # Augment data
    df_estimations = augment_estimation(
        df_estimations=df_estimations,
        min_weeks_per_estm_date=company_config.min_weeks_per_estm_date,
    )

    # Add features
    df_estimations = add_num_days_before_cutoff(
        df_estimations=df_estimations,
        cut_off_dow=company.cut_off_week_day,
    )

    # Convert dtypes to same as pandara schema
    df_estimations = df_estimations.astype(
        {
            col: str(dtype)
            for col, dtype in OrderEstimationFeatures.to_schema().dtypes.items()
        }
    )

    try:
        df_estimations = OrderEstimationFeatures.validate(df_estimations, lazy=True)
        return df_estimations
    except pa.errors.SchemaErrors as err:
        logger.error("Schema errors and failure cases:")
        logger.error(err.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(err.data)
        logger.error("\nMessage:")
        logger.error(err.message)

    return df_estimations


def get_estimations_from_catalog(
    catalog_name: str,
    catalog_schema: str,
    company: Company,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Downloading estimations total log...")
    df_estimations_total = fetch_data_from_sql(
        sql_name="estimations_total",
        directory=SQL_DIR,
        catalog_name=catalog_name,
        catalog_scheme=catalog_schema,
        company_id=company.company_id,
    )
    logger.info(
        f"Estimation total log downloaded with total {df_estimations_total.shape[0]} rows"
    )

    logger.info("Downloading estimations dishes log...")
    df_estimations_dishes = fetch_data_from_sql(
        sql_name="estimations_dishes",
        directory=SQL_DIR,
        catalog_name=catalog_name,
        catalog_scheme=catalog_schema,
        company_id=company.company_id,
    )
    logger.info(
        f"Estimation dishes log downloaded with total {df_estimations_total.shape[0]} rows"
    )

    return df_estimations_total, df_estimations_dishes


def clean_prediction_date_estimations(
    df_estimations: pd.DataFrame, prediction_date: datetime, cut_off_day: int
) -> pd.DataFrame:
    """To remove the estimations that are already passed cut off"""
    # Given a prediction, what is the 1st week number that we're predicting for
    start_year, start_week = get_forecast_start(
        cut_off_day=cut_off_day, start_date=prediction_date
    )
    start_yyyyww = start_year * 100 + start_week
    logger.info(
        f"Cut off day is {cut_off_day}, predictions should start at {start_yyyyww}"
    )

    pred_date_indices = df_estimations[
        df_estimations["estimation_date"] == prediction_date
    ].index

    # Drop the indices where the week is too old on prediction date
    idx_to_drop = df_estimations.loc[pred_date_indices][
        (
            df_estimations.loc[pred_date_indices]["year"] * 100
            + df_estimations.loc[pred_date_indices]["week"]
        )
        < start_yyyyww
    ].index

    df_estimations = df_estimations.drop(idx_to_drop)
    return df_estimations


def preliminarily_clean_data(
    df_estimations_total: pd.DataFrame,
    df_estimations_dishes: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Preliminarily cleaning estimation logs df...")
    df_estimations_total = preliminary_clean_estimations_df(
        df_estimations=df_estimations_total,
        estimation_col="quantity_estimated_total",
    )
    df_estimations_dishes = preliminary_clean_estimations_df(
        df_estimations=df_estimations_dishes,
        estimation_col="quantity_estimated_dishes",
    )
    df_estimations = df_estimations_total.merge(
        df_estimations_dishes[
            [
                "estimation_date",
                "year",
                "week",
                "company_id",
                "quantity_estimated_dishes",
            ]
        ],
        on=["estimation_date", "year", "week", "company_id"],
        how="left",
    )

    df_estimations["quantity_estimated_dishes"] = df_estimations[
        "quantity_estimated_dishes"
    ].fillna(0)

    return df_estimations


def preliminary_clean_estimations_df(
    df_estimations: pd.DataFrame,
    estimation_col: str,
) -> pd.DataFrame:
    df_estimations["estimation_date"] = df_estimations["estimation_timestamp"].dt.date
    df_estimations = df_estimations.drop_duplicates(
        subset=["year", "week", "company_id", "product_type_id", "estimation_date"],
    )

    df_estimations_agg = pd.DataFrame(
        df_estimations.groupby(["year", "week", "estimation_date", "company_id"])[
            estimation_col
        ].sum(),
    ).reset_index()
    df_estimations = df_estimations_agg.sort_values(
        by=["estimation_date", "year", "week", "company_id"],
    )
    df_estimations["estimation_date"] = pd.to_datetime(
        df_estimations["estimation_date"],
    )
    # Remove week 53 from the years that should not have week 53
    extra_yearweek = 53
    df_wk53 = df_estimations[df_estimations["week"] == extra_yearweek].copy()
    if df_wk53.shape[0] >= 1:
        logger.info("Removing week 53 that should not exist..")
        df_wk53["has_week_53"] = df_wk53.apply(
            lambda x: is_year_has_week_53(x["year"]),
            axis=1,
        )
        years_without_week3 = df_wk53[~df_wk53["has_week_53"]]["year"].unique()
        # Exclude the ones that says week 53
        # but the years actually should not have week 53
        df_estimations = df_estimations[
            ~(
                (df_estimations["week"] == extra_yearweek)
                & (df_estimations["year"].isin(years_without_week3))
            )
        ]

    return df_estimations


def augment_estimation(
    df_estimations: pd.DataFrame,
    min_weeks_per_estm_date: int,
) -> pd.DataFrame:
    # Calculate how many weeks each estimation date contain
    df_num_estimation_weeks = pd.DataFrame(
        df_estimations.groupby("estimation_date")["week"].nunique(),
    ).reset_index()
    df_num_estimation_weeks["num_weeks_to_augment"] = (
        min_weeks_per_estm_date - df_num_estimation_weeks["week"]
    )

    df_estimations = df_estimations.sort_values(by=["estimation_date", "year", "week"])

    list_df_augmented = []
    for an_estimation_date, num_weeks_to_augment in df_num_estimation_weeks[
        ["estimation_date", "num_weeks_to_augment"]
    ].values:
        if num_weeks_to_augment > 0:
            df_tmp = df_estimations[
                df_estimations["estimation_date"] == an_estimation_date
            ]
            last_year, last_week = df_tmp.tail(1)[["year", "week"]].values[0]

            df_augmented = create_future_df(
                start_year=last_year,
                start_week=last_week,
                horizon=(num_weeks_to_augment + 1),
            )
            # Remove the first row which is the same as df_estimation's last available date
            df_augmented = df_augmented[1:]

            # Fill the values with all the last known value
            for a_col in df_tmp.columns:
                if a_col not in df_augmented.columns:
                    df_augmented[a_col] = df_tmp.tail(1)[a_col].values[0]
            df_augmented = df_augmented[df_tmp.columns]

            if len(df_augmented) > 0:
                list_df_augmented.append(df_augmented)

    if len(list_df_augmented) == 0:
        logger.warning(
            "No data to augment, returning the original dataframe with no augmentation"
        )
        return df_estimations

    # convert list to be a df
    all_df_augmented = pd.concat(list_df_augmented, ignore_index=True)
    # Concated the augmented dfs with the estimation
    all_df_augmented = all_df_augmented[df_estimations.columns]

    df_estimations = pd.concat(
        [df_estimations, all_df_augmented],
        ignore_index=True,
    ).sort_values(by=["estimation_date", "year", "week"])
    return df_estimations


def add_num_days_before_cutoff(
    df_estimations: pd.DataFrame,
    cut_off_dow: int,
) -> pd.DataFrame:
    df_estimations = get_cut_off_date(df=df_estimations, cut_off_dow=cut_off_dow)

    df_estimations["num_days_to_cut_off"] = (
        df_estimations["cut_off_date"] - df_estimations["estimation_date"]
    ).dt.days
    return df_estimations
