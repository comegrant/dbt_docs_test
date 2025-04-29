# Databricks notebook source
import sys

sys.path.append("../helper_functions")

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "Operations"
tables = [
    "cases"
    ,"case_line"
    ,"case_line_type"
    ,"case_line_ingredient"
    ,"case_category"
    ,"case_responsible"
    ,"case_cause"
]
for table in tables:
    load_coredb_full(dbutils, database, table)
