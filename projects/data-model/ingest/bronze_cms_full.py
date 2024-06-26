# Databricks notebook source
import sys
sys.path.append('../../../packages/data-connector')

# COMMAND ----------

from data_connector.coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = ["company","country"]
for table in tables: 
    load_coredb_full(database, table)
