
import os

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def create_or_replace_table_query(
    host: str, 
    database: str, 
    table: str, 
    schema: str, 
    query: str, 
    user: str, 
    password: str, 
    target_schema: str = None, 
    target_table: str = None
    ) -> None:
    """
    Extract data from AnalyticsDB and load it into a table in the selected target schema in Databricks.

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
    
    if not target_schema:
        target_schema = "bronze"
    
    if not target_table:
        # Default naming convention if target_table is not provided
        final_table_name = f"{database}_{schema}__{table}"
    else:
        final_table_name = target_table
        
    full_table_name = f"{target_schema}.{final_table_name}"
    remote_table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

def load_analyticsdb_query(
    dbutils: DBUtils, 
    database: str, 
    table: str, 
    schema: str, 
    query: str, 
    host: str = "gg-analytics.database.windows.net", 
    target_schema: str = None, 
    target_table: str = None
    ) -> None:
    """
    Executes custom query that loads selected data from a table in AnalyticsDB

    Args:
        host(str): The hostname of the AnalyticsDB server
        database (str): The database in AnalyticsDB to extract data from
        table (str): The table to extract
        schema (str): The schema in AnalyticsDB to extract data from
        query (str): Query to run towards database
        target_schema (str): The target schema in Databricks to write data to
        target_table (str): Optional custom target table name in Databricks
    """

    username = dbutils.secrets.get(scope="auth_common", key="analyticsDb-username")
    password = dbutils.secrets.get(scope="auth_common", key="analyticsDb-password")

    # Update the docstring and add missing parameters to match create_or_replace_table_query
    create_or_replace_table_query(
        host=host,
        database=database,
        table=table,
        schema=schema,
        query=query,
        user=username,
        password=password,
        target_schema=target_schema,
        target_table=target_table
    )
