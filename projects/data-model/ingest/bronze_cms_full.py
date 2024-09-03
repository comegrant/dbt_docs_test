# Databricks notebook source

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = [
        "addon_subscriptions",
        "billing_agreement",
        "billing_agreement_basket",
        "billing_agreement_status",
        "billing_agreement_addon_subscriptions",
        "billing_agreement_order_status",
        "company",
        "country",
        "loyalty_agreement_ledger",
        "loyalty_level",
        "order_type"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
