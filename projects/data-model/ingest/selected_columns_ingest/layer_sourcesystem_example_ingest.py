# Databricks notebook source
# MAGIC %md
# MAGIC This is not a part of the full cms ingest because we avoid including personal info.

# COMMAND ----------

import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_query

# COMMAND ----------

# Name of database in source system
database = ""

# Source table name
source_table = ""

# COMMAND ----------

query = f"""
    SELECT 
        column_name
        , column_name
        , column_name
        , column_name
        , column_name
    FROM {source_table}
"""

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query)
