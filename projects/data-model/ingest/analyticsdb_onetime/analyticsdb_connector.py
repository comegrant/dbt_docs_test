
import os

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def create_or_replace_table_query(host: str, database: str, table: str, schema: str, query: str, user: str, password: str) -> None:
    """
    Extract data from AnalyticsDB and load it into a table the bronze layer in Databricks.

    Args:
        host(str): The hostname of the AnalyticsDB server
        database (str): The database in AnalyticsDB to extract data from
        table (str): The table to extract data from
        query (str): Query to run towards the database in AnalyticsDB
        user (str): The username for AnalyticsDB
        password (str): The password for AnalyticsDB
    """

    spark = SparkSession.builder.getOrCreate()
    remote_table = (spark.read
        .format("sqlserver")
        .option("host", host)
        .option("port", "1433")
        .option("user", user)
        .option("password", password)
        .option("database", database)
        .option("query", query)
        .load()
    )

    database = database.lower()

    remote_table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"bronze.{database}_{schema}__{table}")

def load_analyticsdb_query(dbutils: DBUtils, database: str, table: str, schema: str, query: str, host: str = "gg-analytics.database.windows.net") -> None:
    """
    Executes custom query that loads selected data from a table in AnalyticsDB

    Args:
        host(str): The hostname of the AnalyticsDB server
        database (str): The database in AnalyticsDB to extract data from
        table (str): The table to extract
        query (str): Query to run towards datab
    """

    username = dbutils.secrets.get( scope="auth_common", key="analyticsDb-username" )
    password = dbutils.secrets.get( scope="auth_common", key="analyticsDb-password" )

    create_or_replace_table_query(host, database, table, schema, query, username, password)