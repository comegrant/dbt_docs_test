# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "OPERATIONS"
tables = [
        "orders_history",
        "order_collis_history"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
