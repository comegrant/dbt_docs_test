import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from constants.companies import Company
from databricks.feature_engineering import (
    FeatureEngineeringClient,
    FeatureLookup,
)
from databricks.feature_engineering.training_set import TrainingSet
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as f

from customer_churn.models.features import FeaturesConfig
from customer_churn.models.train import TrainConfig

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()

LABEL_TEXT_CHURNED = "Churned"
LABEL_TEXT_DELETED = "Deleted"
LABEL_TEXT_ACTIVE = "Active"


def load_dataset(
    env: str,
    company: Company,
    feature_config: FeaturesConfig,
    train_config: TrainConfig,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> TrainingSet:
    logger.info("Loading dataset")
    start_date, end_date = get_dataset_dates(
        env=env,
        start_date=start_date,
        end_date=end_date,
    )

    fs = FeatureEngineeringClient()

    feature_lookups = get_feature_lookups(env=env, feature_config=feature_config)
    target_set = get_target_set(env=env, company=company, train_config=train_config)

    logger.info(feature_lookups)

    training_set = fs.create_training_set(
        df=target_set,
        feature_lookups=feature_lookups,
        label=feature_config.target_col,
        exclude_columns=[
            "agreement_id",
            "company_id",
            "planned_delivery",
            "bargain_hunter",
            "sub_segment_name",
        ],
    )

    logger.info(
        "Dataset loaded with columns: %s", training_set.feature_spec.column_infos
    )

    return training_set


def get_target_set(
    env: str,
    company: Company,
    train_config: TrainConfig,
    schema: str = "mlfeaturetesting",
) -> pd.DataFrame:
    logger.info("Loading order feature, target is calculated from future orders")

    df = (
        spark.read.table(f"{env}.{schema}.customer_snapshot_features")
        .filter(f"company_id = '{company.company_id}'")
        .withColumn("agreement_id", f.col("agreement_id").cast("int"))
        .select(
            "agreement_id",
            "company_id",
            "snapshot_date",
            "weeks_since_last_delivery",
        )
    )

    df = add_labels(
        df=df, buy_history_churn_weeks=train_config.buy_history_churn_weeks
    ).select("agreement_id", "company_id", "snapshot_date", "forecast_status")

    return df


def add_labels(df: DataFrame, buy_history_churn_weeks: int) -> DataFrame:
    logger.info("Processing labels...")
    logger.info(df.columns)

    # Set churn window to be next buy history weeks (for example 4 weeks)
    window_spec = (
        Window.partitionBy("agreement_id")
        .orderBy("snapshot_date")
        .rowsBetween(buy_history_churn_weeks - 1, buy_history_churn_weeks)
    )

    # Check if weeks_since_last_delivery is greater than buy_history_churn_weeks
    df = df.withColumn(
        "no_future_orders",
        f.last("weeks_since_last_delivery").over(window_spec) > buy_history_churn_weeks,
    )

    # Define the forecasted status (churn or active) as a result of the window function
    df = df.withColumn(
        "forecast_status",
        f.when(f.col("no_future_orders"), LABEL_TEXT_CHURNED).otherwise(
            LABEL_TEXT_ACTIVE
        ),
    )

    return df


def get_feature_lookups(
    env: str, feature_config: FeaturesConfig
) -> list[FeatureLookup]:
    feature_container_name = feature_config.feature_container_name
    feature_lookups = []
    for feature_table_config in feature_config.feature_tables:
        columns = feature_table_config.columns
        feature_lookups.append(
            FeatureLookup(
                table_name=f"{env}.{feature_container_name}.{feature_table_config.name}",
                lookup_key=columns.primary_keys,
                timestamp_lookup_key=columns.timeseries_columns,
                lookback_window=timedelta(days=7)
                # feature_names=feature_config["feature_columns"],
            )
        )

    return feature_lookups


def get_dataset_dates(
    env: str = "dev",
    schema: str = "mltesting",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> tuple[datetime, datetime]:
    if end_date is None:
        end_date = (
            spark.read.table(f"{env}.{schema}.customer_churn_snapshot")
            .select("snapshot_date")
            .agg(f.max("snapshot_date").alias("max_snapshot_date"))
            .collect()[0]["max_snapshot_date"]
        )
        end_date = pd.to_datetime(end_date)

    if start_date is None:
        start_date = end_date - pd.Timedelta(days=30)

    return start_date, end_date


def create_train_val_split(
    training_set: TrainingSet,
    train_start_yyyyww: int,
    num_holdout_weeks: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Training set to pandas
    training_set = training_set.load_df()
    training_set = training_set.fillna(0)
    df = training_set.toPandas()

    # First only keep the data after train_start (included)
    train_start = train_start_yyyyww
    logger.info(f"Only keeping data after {train_start}...")

    # Convert snapshot_date to year and week
    df["year"] = df["snapshot_date"].dt.year
    df["week"] = df["snapshot_date"].dt.isocalendar().week
    df["yearweek"] = df["year"] * 100 + df["week"]

    df = df[df["yearweek"] >= train_start]

    num_unique_weeks = np.sort(
        (df["yearweek"]).unique(),
    )

    # Check if the number of unique weeks is greater than the number of holdout weeks
    if len(num_unique_weeks) < num_holdout_weeks:
        raise ValueError(
            f"Number of unique weeks {len(num_unique_weeks)} is less than number of holdout weeks {num_holdout_weeks}"
        )

    first_holdout_yyyyww = num_unique_weeks[-num_holdout_weeks]

    df_holdout = df[(df["yearweek"]) >= first_holdout_yyyyww]
    df_train = df[(df["yearweek"]) < first_holdout_yyyyww]

    df.drop(columns=["year", "week", "yearweek"], inplace=True)
    df_holdout.drop(columns=["year", "week", "yearweek"], inplace=True)
    df_train.drop(columns=["year", "week", "yearweek"], inplace=True)

    logger.info(f"Training set shape: {df_train.shape}")
    logger.info(f"Holdout set shape: {df_holdout.shape}")

    return df_train, df_holdout
