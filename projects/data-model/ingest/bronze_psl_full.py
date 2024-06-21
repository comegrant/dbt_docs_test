# Databricks notebook source
from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PRODUCT_LAYER"
tables = ["product"]
for table in tables: 
    load_coredb_full(dbutils, database, table)
