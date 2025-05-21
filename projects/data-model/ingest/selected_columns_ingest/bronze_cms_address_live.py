# Databricks notebook source
# MAGIC %md
# MAGIC This is not a part of the full cms ingest because we avoid including personal info.

# COMMAND ----------
import sys
sys.path.append('../helper_functions')

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_table = "address_live"

# COMMAND ----------

query = """
    SELECT 
        id
        , agreement_id
        , postal_code
        , geo_restricted
        , created_at
        , created_by
        , updated_at
        , updated_by
    FROM address_live
"""

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query)
