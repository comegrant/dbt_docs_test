# Databricks notebook source
from analyticsdb_connector import load_analyticsdb_query

# COMMAND ----------

database = "AnalyticsDB"
schema = "cms"
table = "estimations_log"
query = "SELECT * FROM cms.estimations_log"

# COMMAND ----------

load_analyticsdb_query(dbutils, database, table, schema, query)

# COMMAND ----------

database = "AnalyticsDB"
schema = "cms"
table = "estimations_log_history"
query = "SELECT * FROM cms.estimations_log_history"

# COMMAND ----------

load_analyticsdb_query(dbutils, database, table, schema, query)
