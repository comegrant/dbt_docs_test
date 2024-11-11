# Databricks notebook source
developer = dbutils.widgets.get("firstname_lastname")
layer = "silver"
dbt_schema = f"`~{developer}_{layer}`"

tables = dbutils.widgets.get("table_list")

# COMMAND ----------

# Check if the 'tables' list is empty
if not tables:

    # Retrieve the tables using Spark SQL and store them in the 'tables' variable
    tables_df = spark.sql(f"SHOW TABLES IN dev.{layer}").collect()

    tables = [row["tableName"] for row in tables_df]

else:
   tables = [item.strip() for item in tables.split(",")]

# COMMAND ----------

for table in tables:
  
  query = f"CREATE OR REPLACE TABLE dev.{dbt_schema}.{table} AS SELECT * FROM dev.{layer}.{table}"
  
  print(query)

  spark.sql(query)
