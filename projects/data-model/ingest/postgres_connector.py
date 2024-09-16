
import os

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def load_postgres_full(dbutils: DBUtils, schema: str, table: str) -> None:
    """
    Extract data from the Staging Postgres database and load it into a table the bronze layer in Databricks.

    Args:
        dbutils (DBUtils): Allows for accessing the secret scope
        schema (str): The schema in the staging database where the table to extract exists
        table (str): The table to extract data from
    """
    host = "gg-analytics-staging.postgres.database.azure.com"
    database = "staging"
    username = dbutils.secrets.get( scope="auth_common", key="postgres-staging-username" )
    password = dbutils.secrets.get( scope="auth_common", key="postgres-staging-password" )

    spark = SparkSession.builder.getOrCreate()
    remote_table = (spark.read
        .format("postgresql")
        .option("host", host)
        .option("port", "5432")
        .option("user", username)
        .option("password", password)
        .option("database", database)
        .option("dbtable", f"{schema}.{table}")
        .load()
    )

    database = database.lower()

    remote_table.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"bronze.postgres_{schema}__{table}")