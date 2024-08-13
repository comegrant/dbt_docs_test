# Databricks notebook source
from datetime import datetime, timedelta

from data_connector.coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
table = ""
date_column = ""
number_of_days = 14

# COMMAND ----------

from_date = (datetime.today() - timedelta(days=number_of_days)).strftime('%Y-%m-%d')
to_date = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

query = f"(SELECT * FROM {table} WHERE {date_column} BETWEEN '{from_date}' AND '{to_date}')"

# COMMAND ----------

load_coredb_query(database, table, query)
