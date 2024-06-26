# Databricks notebook source
import sys
sys.path.append('../../../packages/data-connector')

# COMMAND ----------

from datetime import datetime, timedelta

from data_connector.coredb_connector import load_coredb_full

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

load_coredb_query(database, table, query)
