# Databricks notebook source
# temp solution until Mats have fixed docker stuff
import sys

packages = ["../", "../../../packages/data-connector"]
sys.path.extend(packages)

# COMMAND ----------

from data_connector.coredb_connector import load_coredb_full

# COMMAND ----------

database = "PRODUCT_LAYER"
tables = ["product"]
for table in tables: 
    load_coredb_full(database, table)
