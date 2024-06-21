# Databricks notebook source
from datetime import datetime, timedelta

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
table = "billing_agreement_order"
date_column = "created_date"

# COMMAND ----------

#Always 14 days back 
from_date = (datetime.today() - timedelta(days=14)).strftime('%Y-%m-%d')
to_date = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

query = f"(SELECT * FROM {table} WHERE {date_column} BETWEEN '{from_date}' AND '{to_date}')"

# COMMAND ----------

load_coredb_query(dbutils, database, table, query)
