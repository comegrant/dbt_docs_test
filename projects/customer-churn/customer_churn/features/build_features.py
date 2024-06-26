import logging
from datetime import datetime
from importlib import import_module

from constants.companies import Company
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession

from customer_churn.features.customers import get_customers_features_for_date
from customer_churn.models.features import FeaturesConfig, FeatureTableConfig

logger = logging.getLogger(__name__)
spark = SparkSession.getActiveSession()


def generate_features(
    env: str,
    company: Company,
    snapshot_date: datetime,
    features_config: FeaturesConfig,
) -> None:
    """
    Generate features for customers, events, orders, crm segments and complaints
    """
    feature_group_to_include = [
        "events",
        "orders",
        "crm_segments",
        "complaints",
    ]
    feature_table_config = features_config.feature_tables

    fe = FeatureEngineeringClient()
    df_customers_features = get_customers_features_for_date(env, company, snapshot_date)
    feature_columns = feature_table_config[0].columns.feature_columns

    for feature_set in feature_group_to_include:
        logger.info(f"Generating {feature_set} features")
        func = getattr(
            import_module(f"customer_churn.features.{feature_set}"),
            f"get_{feature_set}_features_for_date",
        )
        df = func(env, company, snapshot_date)

        df_customers_features = df_customers_features.join(
            df, on="agreement_id", how="left"
        )

        logger.info(f"Saving {feature_set} features to feature store")

    df_customers_features = df_customers_features.fillna(0, subset=feature_columns)

    logger.info("Saving customers features to feature store")
    save_feature_table(
        fe,
        df_customers_features,
        env,
        feature_container=features_config.feature_container_name,
        feature_table_config=feature_table_config[0],
        feature_columns=feature_columns,
    )


def save_feature_table(
    fe: FeatureEngineeringClient,
    df: DataFrame,
    env: str,
    feature_container: str,
    feature_table_config: FeatureTableConfig,
    feature_columns: list,
) -> None:
    """Save a pyspark dataframe as a feature table in feature store

    Args:
        fe (FeatureEngineeringClient)
        df (DataFrame): pyspark dataframe to be saved
        env (str): the env of the workspace. dev/prod
        feature_container (str): container of the feature table
        feature_table_name (str): the table to be saved into
        primary_keys (list[str]): the primary keys of the table
        table_description (str | None, optional).
    """
    feature_table_name = feature_table_config.name
    primary_keys = feature_table_config.columns.primary_keys
    timeseries_columns = feature_table_config.columns.timeseries_columns
    table_description = feature_table_config.description

    feature_table_name_full = f"{env}.{feature_container}.{feature_table_name}"
    df = df.select(primary_keys + timeseries_columns + feature_columns)

    if not spark.catalog.tableExists(feature_table_name_full):
        logger.info(f"Creating table {feature_table_name_full}...")
        fe.create_table(
            name=feature_table_name_full,
            primary_keys=primary_keys + timeseries_columns,
            df=df,
            schema=df.schema,
            description=table_description,
            timeseries_columns=timeseries_columns,
        )
    logger.info(f"Writing into {feature_table_name_full}...")
    fe.write_table(name=feature_table_name_full, df=df, mode="merge")
