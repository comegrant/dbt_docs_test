# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
tables = [
        "addon_subscriptions",
        "billing_agreement_basket_deviation_origin",
        "billing_agreement_status",
        "billing_agreement_addon_subscriptions",
        "billing_agreement_order_discount",
        "billing_agreement_order_status",
        "company",
        "country",
        "consent",
        "campaign",
        "campaign_location",
        "campaign_code",
        "category_consent",
        "delivery_week_type",
        "discount",
        "discount_amount_type",
        "discount_category",
        "discount_category_mapping_discount_sub_category",
        "discount_chain",
        "discount_channel",
        "discount_coupon",
        "discount_coupon_type",
        "discount_mapping_channel_category",
        "discount_sub_category",
        "discount_type",
        "discount_usage_type",
        "loyalty_agreement_ledger",
        "loyalty_event",
        "loyalty_level",
        "loyalty_order",
        "loyalty_order_line",
        "loyalty_order_status",
        "order_type",
        "preference",
        "preference_company",
        "preference_type",
        "preference_attribute_value",
        "register_info_products"
        ]
for table in tables:
    load_coredb_full(dbutils, database, table)
