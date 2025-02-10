from pathlib import Path
from typing import Any

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession


def get_spark_session() -> SparkSession:
    """Create a spark session."""
    try:
        from databricks.sdk.runtime import spark

        return spark
    except AttributeError:
        return DatabricksSession.builder.getOrCreate()


def get_serverless_spark_session() -> SparkSession:
    """Create a serverless spark session."""
    return DatabricksSession.builder.serverless().getOrCreate()


def get_data_from_sql(spark: DatabricksSession, sql_path: Path, **kwargs: Any) -> DataFrame:  # noqa
    """Get data from SQL query file."""
    with Path(sql_path).open() as f:
        custom_query = f.read().format(**kwargs)
    df = spark.sql(custom_query)  # type: ignore
    return df
