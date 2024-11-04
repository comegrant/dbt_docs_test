# Databricks notebook source
from analyticsdb_connector import load_analyticsdb_query

# COMMAND ----------

database = "AnalyticsDB"
schema = "analytics"
table = "billing_agreement_basket_product_log"
query = f"SELECT * FROM {schema}.{table}"

# COMMAND ----------

load_analyticsdb_query(dbutils, database, table, schema, query)