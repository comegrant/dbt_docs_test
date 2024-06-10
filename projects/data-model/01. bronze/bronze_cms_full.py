# Databricks notebook source
import sys
sys.path.append('./../reusable')
from ingest import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = ["company","country"]
for table in tables: 
    load_coredb_full(dbutils, database, table)
