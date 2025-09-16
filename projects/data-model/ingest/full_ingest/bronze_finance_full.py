# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "FINANCE"
tables = [
        "invoice_type",
        "invoice_desc",
        "invoice_process_status",
        "payment_partner"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
