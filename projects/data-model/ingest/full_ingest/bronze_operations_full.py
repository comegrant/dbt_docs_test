# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "OPERATIONS"
tables = [
        "case_category"
        , "case_cause"
        , "case_line_ingredient"
        , "case_line_type"
        , "case_responsible"
        , "case_taxonomies"
        , "taxonomies"
        , "cutoff"
        , "cutoff_calendar"
        , "last_mile_route"
        , "order_collis"
        , "distribution_center"
        , "distribution_center_type"
        , "timeblock_blacklist"
        , "transport_company"
        , "routes"
        , "zones"
        , "zones_blacklist"
        , "zone_postcode"
        ]

for table in tables:
    load_coredb_full(dbutils, database, table)
