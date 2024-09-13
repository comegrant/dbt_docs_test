# Databricks notebook source
developer = "firstname_lastname"
schema = "gold"
db_name = f"dev.{developer}_{schema}"

tables = spark.sql(f"SHOW TABLES IN {db_name}")


# COMMAND ----------

tables.show()

# COMMAND ----------

for row in tables.collect():
  spark.sql(f"DROP TABLE {db_name}.{row[1]}")
