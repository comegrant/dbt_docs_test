import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def is_running_databricks() -> bool:
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


if not is_running_databricks():
    from databricks import sql
    from databricks.sql.client import Connection
    from databricks.sql.experimental.oauth_persistence import DevOnlyFilePersistence

try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    logger.warning("Failed to import DatabricksSession, using default pyspark")
    from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def connect(
    databricks_server_hostname: str = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    databricks_http_path: str = os.getenv("DATABRICKS_HTTP_PATH"),
) -> "Connection":
    """
    Connect to a databricks server.

    Args:
        databricks_server_hostname (str): The hostname of the databricks server.
        databricks_http_path (str): The HTTP path of the databricks SQL server (starting with /sql/).

    Returns:
        Connection: A connection object to the databricks server.

    Raises:
        ValueError: If DATABRICKS_SERVER_HOSTNAME or DATABRICKS_HTTP_PATH are not set.
    """
    if not databricks_server_hostname:
        databricks_server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    if not databricks_http_path:
        databricks_http_path = os.getenv("DATABRICKS_HTTP_PATH")

    # Check if DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HTTP_PATH are set
    if not databricks_server_hostname or not databricks_http_path:
        raise ValueError(
            "DATABRICKS_SERVER_HOSTNAME and DATABRICKS_HTTP_PATH must be set to connect to databricks",
        )

    # Connect to databricks
    return sql.connect(
        server_hostname=databricks_server_hostname,
        http_path=databricks_http_path,
        auth_type="databricks-oauth",
        experimental_oauth_persistence=DevOnlyFilePersistence("./connection.json"),
    )


def save_dataframe_to_table(
    dataframe: pd.DataFrame,
    table_name: str,
    schema: str,
    mode: str = "errorifexists",
    catalog_name: str | None = None,
) -> None:
    """
    Saves a Pandas DataFrame to a table in Spark.

    Args:
        dataframe (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the table to save to.
        schema (str): The schema of the table.
        mode (str): The mode for saving the data.
        catalog_name (str | None, optional): The catalog name for the table. Defaults to None.

    Returns:
        None
    """
    # Use Spark to write data
    spark = get_spark_session()
    df = spark.createDataFrame(dataframe)
    df.write.mode(mode).saveAsTable(
        f"{catalog_name + '.' if catalog_name else ''}{schema}.{table_name}",
    )


def read_data_spark(
    query: str,
) -> pd.DataFrame:
    """
    Reads data from a Spark SQL query and returns a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: The resulting DataFrame from the query.
    """
    # Use Spark to read data
    spark = get_spark_session()
    return spark.sql(query).toPandas()


def read_data_databricks_sql(query: str) -> pd.DataFrame:
    """
    Reads data from a Databricks SQL query and returns a pandas DataFrame.

    Args:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: The resulting DataFrame from the query.
    """
    # Connect to databricks
    connection = connect()
    cursor = connection.cursor()

    # Execute query
    cursor.execute(query)

    # Convert sql result to DataFrame
    res = cursor.fetchall()

    # close connection
    cursor.close()
    connection.close()

    if len(res) == 0:
        logger.warning("SQL WARNING: No data returned")
        return pd.DataFrame()

    return pd.DataFrame(res, columns=res[0].asDict().keys())


def read_data(query: str) -> pd.DataFrame:
    """
    Reads data from either a Spark SQL query or a Databricks SQL query and returns a pandas DataFrame.

    Parameters:
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: The resulting DataFrame from the query.
    """
    if is_running_databricks():
        return read_data_spark(query)
    else:
        return read_data_databricks_sql(query)
