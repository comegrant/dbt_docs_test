# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PRODUCT_LAYER"
tables = [
    "product", 
    "product_concept",
    "product_type", 
    "product_type_concept",
    "product_status",
    "product_variation", 
    "product_variation_company", 
    "product_variation_attribute_template", 
    "product_variation_attribute_value"
    ]
for table in tables: 
    load_coredb_full(dbutils, database, table)
