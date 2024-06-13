import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import SparkSession

from dishes_forecasting.logger_config import logger

spark = SparkSession.getActiveSession()


def save_feature_table(
    fe: FeatureEngineeringClient,
    df: pd.DataFrame,
    env: str,
    feature_container: str,
    feature_table_name: str,
    columns: dict,
    table_description: str | None = None,
) -> None:
    """Save a pandas dataframe as a feature table in feature store

    Args:
        fe (FeatureEngineeringClient)
        df (pd.DataFrame): pandas dataframe to be saved
        env (str): the env of the workspace. dev/prod
        feature_container (str): container of the feature table
        feature_table_name (str): the table to be saved into
        primary_keys (list[str]): the primary keys of the table
        table_description (str | None, optional).
    """
    feature_table_name_full = f"{env}.{feature_container}.{feature_table_name}"
    primary_keys = columns["primary_keys"]
    feature_columns = columns["feature_columns"]
    df = df[primary_keys + feature_columns]
    spark_df = spark.createDataFrame(df)
    if not spark.catalog.tableExists(feature_table_name_full):
        logger.info(f"Creating table {feature_table_name_full}...")
        fe.create_table(
            name=feature_table_name_full,
            primary_keys=primary_keys,
            df=spark_df,
            schema=spark_df.schema,
            description=table_description,
        )
    logger.info(f"Writing into {feature_table_name_full}...")
    fe.write_table(name=feature_table_name_full, df=spark_df, mode="merge")
