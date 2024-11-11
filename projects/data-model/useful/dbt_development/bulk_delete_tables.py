# Databricks notebook source
developer = dbutils.widgets.get("firstname_lastname")
layer = dbutils.widgets.get("layer")
dbt_schema = f"`~{developer}_{layer}`"

tables = spark.sql(f"SHOW TABLES IN {dbt_schema}")

# COMMAND ----------

tables.show()

# COMMAND ----------

for table in tables.collect():
  spark.sql(f"DROP TABLE {dbt_schema}.{table[1]}")
