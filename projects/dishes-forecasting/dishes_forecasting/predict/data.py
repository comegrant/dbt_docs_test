import logging

import pandas as pd
from constants.companies import Company
from pyspark.sql import DataFrame, SparkSession

from dishes_forecasting.train.configs.feature_lookup_config import FeatureLookUpConfig


def create_pred_dataset(
    spark: SparkSession,
    env: str,
    min_yyyyww: int,
    company: Company,
    feature_lookup_config_list: FeatureLookUpConfig
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_left = download_weekly_variations(
        spark=spark,
        company=company,
        min_yyyyww=min_yyyyww,
        env=env,
    )
    df_list = []
    ignore_columns = []
    for a_feature_lookup_config in feature_lookup_config_list:
        feature_container = a_feature_lookup_config.features_container
        feature_table_name = a_feature_lookup_config.feature_table_name
        feature_columns = (
            a_feature_lookup_config.primary_keys
            + a_feature_lookup_config.feature_columns
        )
        table_name = f"{env}.{feature_container}.{feature_table_name}"
        logging.info(f"Downloading data from {table_name} ...")
        df = (
            spark
            .read
            .table(table_name)
            .select(feature_columns)
        )
        df_list.append(df.toPandas())
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)
    ignore_columns = list(set(ignore_columns))

    df_merged = df_left
    for df in df_list:
        df_merged = df_merged.merge(df, how="left")
    df_merged = df_merged.dropna()
    return df_merged, df_left


def download_weekly_variations(
    spark: SparkSession,
    company: Company,
    min_yyyyww: int,
    env: str,
) -> DataFrame:
    company_id = company.company_id
    df_left = (
        spark.read.table(f"{env}.mlfeatures.ft_weekly_dishes_variations")
        .select(
            "menu_year",
            "menu_week",
            "company_id",
            "product_variation_id",
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"menu_year * 100 + menu_week >= {min_yyyyww}")
    ).toPandas()

    return df_left
