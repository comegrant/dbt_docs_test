# Databricks notebook source
import sys
sys.path.append('../../reusable')

from analyticsdb_connector import load_analyticsdb_query

# COMMAND ----------

# Run the relevant cell below before running this cell
database = "AnalyticsDB"
query = f"SELECT * FROM {schema}.{table}"

load_analyticsdb_query(dbutils, database, table, schema, query)

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

schema = "cms"
table = "estimations_log"

# COMMAND ----------

schema = "cms"
table = "estimations_log_history"