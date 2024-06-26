import logging

import numpy as np
import pandas as pd
import pyspark
from constants.companies import Company
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.feature_engineering.training_set import TrainingSet
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from orders_forecasting.models.features import FeaturesConfig

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def load_dataset(
    env: str, company: Company, features_config: FeaturesConfig, target_col: str
) -> TrainingSet:
    logger.info("Loading dataset...")

    feature_lookups = create_feature_lookups(env=env, features_config=features_config)

    fs = FeatureEngineeringClient()
    target_set = get_target_set(
        env=env,
        schema=features_config.target_container,
        target_table=features_config.target_table.name,
        company=company,
        target_col=target_col,
    )

    logger.info(feature_lookups)

    training_set = fs.create_training_set(
        df=target_set,
        feature_lookups=feature_lookups,
        label=target_col,
        exclude_columns=[
            "company_id",
            "estimation_date",
            "first_date_of_week",
            "cut_off_date",
        ],
    )

    logger.info(
        "Dataset loaded with columns %s", training_set.feature_spec.column_infos
    )
    return training_set


def get_target_set(
    env: str, schema: str, target_table: str, company: Company, target_col: str
) -> pyspark.sql.dataframe.DataFrame:
    # Get all dates we have made estimations on
    df_estimations = (
        spark.read.table(f"{env}.mltesting.estimations_total")
        .select(f.col("estimation_timestamp").alias("estimation_date"), "year", "week")
        .filter(f"company_id = '{company.company_id}'")
        .filter("estimation_date > '2020-01-01'")
        .orderBy("year", "week")
    )

    df_estimations = df_estimations.withColumn(
        "estimation_date", f.date_trunc("day", "estimation_date")
    )

    # Join calendar on orders estimation table
    df_target = (
        spark.read.table(f"{env}.{schema}.{target_table}")
        .select("year", "week", "company_id", target_col)
        .filter(f"company_id = '{company.company_id}'")
        .orderBy("year", "week")
    )

    # Merge target on estimations
    df_target = df_target.join(df_estimations, on=["year", "week"], how="left")

    return df_target


def create_feature_lookups(
    env: str, features_config: FeaturesConfig
) -> list[FeatureLookup]:
    feature_lookups = []
    container = features_config.feature_container_name
    for feature_table in features_config.feature_tables:
        # get the content of the dictionary
        columns = feature_table.columns
        table_name = feature_table.name
        full_table_name = f"{env}.{container}.{table_name}"
        feature_lookups.append(
            FeatureLookup(
                table_name=full_table_name,
                lookup_key=columns.primary_keys,
                timestamp_lookup_key=columns.timeseries_columns,
                feature_names=columns.feature_columns,
            ),
        )
    return feature_lookups


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
    logger.info(f"Only keeping data after {train_start_yyyyww}...")

    df = df[df["year"] * 100 + df["week"] >= train_start_yyyyww]

    first_holdout_yyyyww = np.sort(
        (df["year"] * 100 + df["week"]).unique(),
    )[-num_holdout_weeks]

    df_holdout = df[(df["year"] * 100 + df["week"]) >= first_holdout_yyyyww]
    df_train = df[(df["year"] * 100 + df["week"]) < first_holdout_yyyyww]

    logger.info(f"Training set shape: {df_train.shape}")
    logger.info(f"Holdout set shape: {df_holdout.shape}")

    return df_train, df_holdout
