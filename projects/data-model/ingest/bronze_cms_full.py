# Databricks notebook source
from coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = [
        "addon_subscriptions",
        "billing_agreement",
        "billing_agreement_basket",
        "billing_agreement_basket_product",
        "billing_agreement_basket_deviation_origin",
        "billing_agreement_status",
        "billing_agreement_addon_subscriptions",
        "billing_agreement_order_status",
        "billing_agreement_preference",
        "company",
        "country",
        "delivery_week_type",
        "loyalty_agreement_ledger",
        "loyalty_level",
        "onesub_beta_agreements",
        "order_type",
        "preference",
        "preference_company",
        "preference_type",
        "preference_attribute_value",
        "register_info_products"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
