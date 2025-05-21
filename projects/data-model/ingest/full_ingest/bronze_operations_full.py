# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "OPERATIONS"
tables = [
        "case_category"
        , "case_cause"
        , "case_line_ingredient"
        , "case_line_type"
        , "case_responsible"
        , "postal_codes"
        , "timeblock_blacklist"
        , "zones"
        , "zones_blacklist"
        , "zone_postcode"
        ]

for table in tables:
    load_coredb_full(dbutils, database, table)
