# Databricks notebook source

# This notebook is run when a PR is closed to clean up the schemas created for that PR.
pr_number = dbutils.widgets.get("pr_number")
schema_prefix = f"~pr{pr_number}_"
schemas = spark.sql(f"SHOW SCHEMAS IN `dev`").collect()
schema_list = [row['databaseName'] for row in schemas]

# COMMAND ----------

for schema in schema_list:
    if schema.startswith(schema_prefix):
        print(f"Dropping schema: {schema}")
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        except Exception as e:
            print(f"Failed to drop schema {schema}, with error: {e}")
