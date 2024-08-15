
import os

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def create_or_replace_table_query(host: str, database: str, table: str, query: str, user: str, password: str) -> None:
    """
    Extract data from CoreDB and load it into a table the bronze layer in Databricks.

    Args:
        host(str): The hostname of the CoreDB server
        database (str): The database in CoreDB to extract data from
        table (str): The table to extract data from
        query (str): Query to run towards the database in CoreDB
        user (str): The username for CoreDB
        password (str): The password for CoreDB
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

    remote_table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"bronze.{database}__{table}")

def load_coredb_full(dbutils: DBUtils, database: str, table: str) -> None:
    """
    Executes query that loads all rows from a table

    Args:
        database (str): The database in CoreDB to extract data from
        table (str): The table to extract
    """

    query = f"(SELECT * FROM {table})"
    load_coredb_query(dbutils, database, table, query)

def load_coredb_query(dbutils: DBUtils, database: str, table: str, query: str, host: str = "bh-replica.database.windows.net") -> None:
    """
    Executes custom query that loads selected data from a table in CoreDB

    Args:
        host(str): The hostname of the CoreDB server
        database (str): The database in CoreDB to extract data from
        table (str): The table to extract
        query (str): Query to run towards datab
    """

    username = dbutils.secrets.get( scope="auth_common", key="coreDb-replica-username" )
    password = dbutils.secrets.get( scope="auth_common", key="coreDb-replica-password" )

    create_or_replace_table_query(host, database, table, query, username, password)
