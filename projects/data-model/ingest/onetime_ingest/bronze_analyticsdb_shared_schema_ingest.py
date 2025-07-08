# Databricks notebook source
import sys
sys.path.append('../../reusable')

from analyticsdb_connector import load_analyticsdb_query

# COMMAND ----------

# Run the relevant cell below before running this cell
database = "AnalyticsDB"
schema = "shared"
tables = [
    "budget"
    , "budget_marketing_input"
    , "budget_parameter"
    , "budget_parameter_split"
    , "budget_type"
]

# COMMAND ----------
for table in tables:
    query = f"SELECT * FROM {schema}.{table}"
    load_analyticsdb_query(dbutils, database, table, schema, query)