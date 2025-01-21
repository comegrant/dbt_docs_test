# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from coredb_connector import load_coredb_full

# COMMAND ----------

table_dictionary = {
        "CMS":[
                "billing_agreement",
                "billing_agreement_basket",
                "billing_agreement_basket_product",
                "billing_agreement_consent",
                "billing_agreement_preference"
        ],
        "PIM":[
                "recipes_taxonomies",
                "taxonomies",
                "recipe_favorites"
        ]
}

for database, tables in table_dictionary.items():
        for table in tables:
            load_coredb_full(dbutils, database, table)
