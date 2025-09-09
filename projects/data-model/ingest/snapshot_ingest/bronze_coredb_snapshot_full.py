# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

table_dictionary = {
        "CMS":[
                "billing_agreement",
                "billing_agreement_basket",
                "billing_agreement_basket_product",
                "billing_agreement_consent",
                "billing_agreement_preference",
                "loyalty_level_company"
        ],
        "PIM":[
                "recipes_taxonomies",
                "taxonomies",
                "recipe_favorites"
        ],
        "OPERATIONS":[
               "geofence",
               "geofence_postalcodes"
        ]
        # "PARTNERSHIP":[
        #         "billing_agreement_partnership"
        # ]
}

for database, tables in table_dictionary.items():
        for table in tables:
            load_coredb_full(dbutils, database, table)
