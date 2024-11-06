# Databricks notebook source
from coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = [
        "billing_agreement",
        "billing_agreement_basket",
        "billing_agreement_basket_product",
        "billing_agreement_consent",
        "billing_agreement_preference"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
