# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PARTNERSHIP"
tables = [
        "billing_agreement_partnership_loyalty_points"
        , "company_partnership"
        , "company_partnership_rule"
        , "partnership"
        , "partnership_rule"
        ]

for table in tables:
    load_coredb_full(dbutils, database, table)