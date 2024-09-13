
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def create_spark_context() -> SparkSession:
    """
        Create a spark session
    """
    try:
        from databricks.sdk.runtime import spark
        return spark
    except AttributeError:
        return DatabricksSession.builder.getOrCreate()
