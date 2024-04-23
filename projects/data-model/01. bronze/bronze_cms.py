# Databricks notebook source
database_name = "CMS"
tables = ["company","country"]
host = "brandhub-fog.database.windows.net"
username = dbutils.secrets.get('auth_common','coreDbUsername')
password = dbutils.secrets.get('auth_common','coreDbPassword')

# COMMAND ----------

for table in tables:
  remote_table = (spark.read
    .format("sqlserver")
    .option("host", host)
    .option("port", "1433")
    .option("user", username)
    .option("password", password)
    .option("database", database_name)
    .option("dbtable", table)
    .load()
  )

  print(f"dev.bronze.{database_name}_{table}")

  remote_table.write.mode("overwrite").saveAsTable(f"dev.bronze.{database_name}_{table}")
