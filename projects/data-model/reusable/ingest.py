from pyspark.sql import SparkSession

def create_or_replace_table_query(host, database, table, query, user, password):
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

    print(f"bronze.{database}_{table}")
    print(query)

    remote_table.write.mode("overwrite").saveAsTable(f"bronze.{database}_{table}")

def load_coredb_full(dbutils, database, table):
    query = f"(SELECT * FROM {table})"
    load_coredb_query(dbutils, database, table, query)

def load_coredb_query(dbutils, database, table, query):
    host = "brandhub-fog.database.windows.net"
    username = dbutils.secrets.get('auth_common','coreDbUsername')
    password = dbutils.secrets.get('auth_common','coreDbPassword')
    create_or_replace_table_query(host, database, table, query, username, password)