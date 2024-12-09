# Databricks notebook source
from analyticsdb_connector import load_analyticsdb_query

# COMMAND ----------

schema = "analytics"
table = "billing_agreement_basket_product_log"

# COMMAND ----------

schema = "orders"
table = "historical_orders_combined"

# COMMAND ----------

schema = "orders"
table = "historical_order_lines_combined"

# COMMAND ----------

schema = "snapshots"
table = "agreement_status"

# COMMAND ----------

database = "AnalyticsDB"
query = f"SELECT * FROM {schema}.{table}"

load_analyticsdb_query(dbutils, database, table, schema, query)
