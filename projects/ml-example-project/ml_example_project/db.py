import logging
from pathlib import Path

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession


def get_spark_session() -> SparkSession:
    """Create a spark session."""
    try:
        from databricks.sdk.runtime import spark

        return spark
    except AttributeError:
        return DatabricksSession.builder.getOrCreate()


def get_serverless_spark_session() -> DatabricksSession:
    """Create a serverless spark session."""
    return DatabricksSession.builder.serverless().getOrCreate()


def get_data_from_sql(spark: SparkSession, sql_path: Path, **kwargs: dict) -> DataFrame:
    """Get data from SQL query file."""
    with Path(sql_path).open() as f:
        custom_query = f.read().format(**kwargs)
    df = spark.sql(custom_query)
    return df


def save_outputs(
    spark_df: DataFrame,
    table_name: str,
    table_schema: str = "mloutputs",
) -> None:
    """Save (append) data in databricks table.

    Parameters:
        spark_df (DataFrame): Spark dataframe to save.
        table_name (str): Name of the table to write to in Databricks.
        table_schema (str): Name of schema to write to in Databricks.
    """
    full_table_name = f"{table_schema}.{table_name}"
    logging.info(f"Writing into {full_table_name}...")
    spark_df.write.mode("append").saveAsTable(full_table_name)
