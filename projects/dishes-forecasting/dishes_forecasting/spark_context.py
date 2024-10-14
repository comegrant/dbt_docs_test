
from pyspark.sql import SparkSession

from databricks.connect import DatabricksSession


def create_spark_context() -> SparkSession:
    """
        Create a spark session
    """
    try:
        from databricks.sdk.runtime import spark
        return spark
    except AttributeError:
        return DatabricksSession.builder.getOrCreate()
