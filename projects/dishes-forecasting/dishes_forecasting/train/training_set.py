import logging
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from databricks.feature_store import FeatureLookup, FeatureStoreClient
from databricks.feature_store.training_set import TrainingSet
from dishes_forecasting.train.configs.feature_lookup_config import (
    FeatureLookUpConfig,
)


def create_training_set(
    is_use_feature_store: bool,
    env: str,
    company_id: str,
    train_config: dict,
    spark: SparkSession,
    feature_lookup_config_list: list[FeatureLookUpConfig],
    fs: Optional[FeatureStoreClient] = None,
    is_drop_ignored_columns: Optional[bool] = True,
) -> tuple[TrainingSet, pd.DataFrame]:
    if is_use_feature_store:
        training_set, df_training_pk_target = create_training_dataset_databricks(
            env=env,
            company_id=company_id,
            train_config=train_config,
            fs=fs,
            spark=spark,
            feature_lookup_config_list=feature_lookup_config_list,
        )
    else:
        training_set, df_training_pk_target = create_training_data_set_locally(
            env=env,
            company_id=company_id,
            train_config=train_config,
            spark=spark,
            feature_lookup_config_list=feature_lookup_config_list,
            is_drop_ignored_columns=is_drop_ignored_columns,
        )
    return training_set, df_training_pk_target


def create_training_dataset_databricks(
    env: str,
    company_id: str,
    train_config: dict,
    fs: FeatureStoreClient,
    spark: SparkSession,
    feature_lookup_config_list: list[FeatureLookUpConfig],
) -> tuple[TrainingSet, pd.DataFrame]:
    df_training_pk_target = get_training_pk_target(
        spark=spark,
        env=env,
        company_id=company_id,
        min_yyyyww=train_config["train_start_yyyyww"],
        max_yyyyww=train_config["train_end_yyyyww"],
        is_training_set=True,
    )

    feature_lookups, ignore_columns = create_feature_lookups(
        env=env,
        feature_lookup_config_list=feature_lookup_config_list
    )

    training_set = fs.create_training_set(
        spark.createDataFrame(
            df_training_pk_target,
        ),
        feature_lookups=feature_lookups,
        label="variation_ratio",
        exclude_columns=ignore_columns,
    )
    return training_set, df_training_pk_target


def create_training_data_set_locally(
    env: str,
    company_id: str,
    train_config: dict,
    spark: SparkSession,
    feature_lookup_config_list: list[FeatureLookUpConfig],
    is_drop_ignored_columns: Optional[bool] = True,
) -> pd.DataFrame:
    """Use it as an alternative to create_training_data_set, when not running on databricks.

    Args:
        env (str): environment
        company_id (str): company id
        train_config (dict): parameters for training data
        spark (SparkSession): spark session
        exclude_columns_list (Optional[list[str]], optional): columns that are not included in the training set
        (but are necessary for the feature lookup). Defaults to [].

    Returns:
        pd.DataFrame: The merged dataframe of training features and target.
    """
    df_training_pk_target = get_training_pk_target(
        spark=spark,
        env=env,
        company_id=company_id,
        min_yyyyww=train_config["train_start_yyyyww"],
        max_yyyyww=train_config["train_end_yyyyww"],
        is_training_set=True,
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
        df = df.toPandas()
        df_list.append(df)
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)
    ignore_columns = list(set(ignore_columns))
    # Merge all the tables together
    df_merged = df_training_pk_target
    for df in df_list:
        df_merged = df_merged.merge(df, how="left")

    if (ignore_columns is not None) & is_drop_ignored_columns:
        df_merged = df_merged.drop(columns=ignore_columns)
    return df_merged, df_training_pk_target


def get_training_pk_target(
    spark: SparkSession,
    env: str,
    company_id: str,
    min_yyyyww: int,
    max_yyyyww: Optional[int] = None,
    is_training_set: Optional[bool] = True,
) -> pd.DataFrame:
    if max_yyyyww is None:
        # just make it a ridiculously big number
        max_yyyyww = min_yyyyww + 1000000
    logging.info(f"Getting training data for company {company_id} ...")
    df_training_pk_target = (
        spark.read.table(f"{env}.mlgold.dishes_order_history")
        .select(
            "menu_year",
            "menu_week",
            "company_id",
            "product_variation_id",
            "variation_ratio",
            "total_weekly_qty",
            "product_variation_quantity"
        )
        .filter(f"company_id = '{company_id}'")
        .filter(f"menu_year * 100 + menu_week >= {min_yyyyww}")
        .filter(f"menu_year * 100 + menu_week <= {max_yyyyww}")
        .orderBy("menu_year", "menu_week")
        .toPandas()
    )
    if is_training_set:
        df_training_pk_target = df_training_pk_target.drop(
            columns=[
                "total_weekly_qty",
                "product_variation_quantity"
            ]
        )
    df_training_pk_target = df_training_pk_target.dropna(subset="variation_ratio")
    df_training_pk_target["menu_year"] = df_training_pk_target["menu_year"].astype("short")
    df_training_pk_target["menu_week"] = df_training_pk_target["menu_week"].astype("short")
    return df_training_pk_target


def create_feature_lookups(
    env: str,
    feature_lookup_config_list: list[FeatureLookUpConfig],
) -> tuple[list[FeatureLookup], list[str]]:
    """Create feature lookups for the training set. For use in databricks."""
    feature_lookups = []
    ignore_columns = []
    for a_feature_lookup_config in feature_lookup_config_list:
        # the key is the table name
        table_name = a_feature_lookup_config.feature_table_name
        container = a_feature_lookup_config.features_container
        full_table_name = f"{env}.{container}.{table_name}"
        feature_lookups.append(
            FeatureLookup(
                table_name=full_table_name,
                lookup_key=a_feature_lookup_config.primary_keys,
                feature_names=a_feature_lookup_config.feature_columns,
            ),
        )
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)
    ignore_columns = list(set(ignore_columns))
    return feature_lookups, ignore_columns
