from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pytz
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession


def get_data_from_catalog(
    spark: SparkSession,
    env: str,
    table_name: str,
    schema: Optional[str] = "mlgold",
    is_convert_to_pandas: Optional[bool] = False,
    column_list: Optional[list] = None,
) -> Union[DataFrame, pd.DataFrame]:
    """ Retrieve dataframe from the catalog

    Args:
        env (str): the environment you are running in
        table_name (str): name of the table in the catalog. i.e., dev, prod
        schema (Optional[str]): the schema where the table belongs to. i.e., silver.
            Default to mlgold if not specified
        is_convert_to_pandas (Optional[bool]): should the returned dataframe be a pandas dataframe.
            Default to False.
        column_list (Optional[list], optional): The list of columns to fetch.
            If not specified, all columns will be fetched

    Returns:
        DataFrame | pd.DataFrame:
            - it returns a spark DataFrame if is_convert_to_pandas is False.
            - a pandas DataFrame is is_convert_to_pandas is True.
    """

    if column_list is None:

        columns = "*"
    else:
        columns = ", ".join(column_list)
    query = f"""
        SELECT
            {columns}
        FROM
            {env}.{schema}.{table_name}

    """
    df = spark.sql(query)
    if is_convert_to_pandas:
        df = df.toPandas()
    return df


def run_custom_sql(
    spark: SparkSession,
    sql_path: str,
    is_convert_to_pandas: Optional[bool] = False,
    **kwargs: Optional[dict],
) -> Union[DataFrame, pd.DataFrame]:
    with Path(sql_path).open() as f:
        custom_query = f.read().format(**kwargs)
    df = spark.sql(custom_query)
    if is_convert_to_pandas:
        df = df.toPandas()
    return df


def save_df_as_feature_table(
    df: Union[pd.DataFrame, DataFrame],
    fe: FeatureEngineeringClient,
    spark: SparkSession,
    env: str,
    feature_table_schema: str,
    feature_table_name: str,
    primary_keys: list[str],
    is_drop_existing: Optional[bool] = False
) -> None:
    """Create or update a feature table

    Args:
        df (Union[pd.DataFrame, DataFrame]): dataframe either
            in the pandas or spark format
        fe (FeatureEngineeringClient)
        env (str): dev, test, prod
        feature_table_schema (str): the schema to keep your feature table
        feature_table_name (str): name of your feature table
        primary_keys (list[str]): column names to be specified as PKs
    """
    feature_table_name_full = f"{env}.{feature_table_schema}.{feature_table_name}"
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame(df)
    # Check if dataframe has duplicates
    duplicates = df.groupBy(primary_keys).count().where("count > 1")
    if duplicates.count() > 0:
        df = df.dropDuplicates()
    if is_drop_existing:
        spark.sql(
            f"""
                DROP TABLE IF EXISTS {feature_table_name_full}
            """
        )
    if not spark.catalog.tableExists(feature_table_name_full):
        fe.create_table(
            name=feature_table_name_full,
            primary_keys=primary_keys,
            schema=df.schema
        )
    fe.write_table(name=feature_table_name_full, df=df, mode="merge")


def add_updated_at(
    df: pd.DataFrame,
    timezone: Optional[str] = "CET"
) -> pd.DataFrame:
    cet_tz = pytz.timezone(timezone)
    if "updated_at" not in df.columns:
        # Add a column
        df = df.assign(updated_at=datetime.now(tz=cet_tz))
    else:
        # Update the column with the current time stamp
        df.loc[:, "updated_at"] = datetime.now(tz=cet_tz)
    return df
