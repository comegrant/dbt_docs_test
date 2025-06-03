# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from analyticsdb_connector import load_analyticsdb_query



# COMMAND ----------

database = "AnalyticsDB"
schema = "analytics"
target_schema = "forecasting"

tables = [
    {
    "source":"marketing_forecast_variations"
     , "target":"forecast_external_estimates"
     }
    , {
    "source":"marketing_forecast_variations_history"
    , "target":"forecast_external_estimates_history"
    }
]

for table in tables: 
    source_table = table["source"]
    target_table = table["target"]
    query = f"select * from {schema}.{source_table}"
    
    load_analyticsdb_query(dbutils, database, source_table, schema, query, target_schema=target_schema, target_table=target_table)
