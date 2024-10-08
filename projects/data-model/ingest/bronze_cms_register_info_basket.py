# Databricks notebook source
# MAGIC %md
# MAGIC This is not a part of the full cms ingest because we avoid including personal info.

# COMMAND ----------

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_table = "register_info_basket"
silver_table = f"dev.silver.cms__{source_table}s"

# COMMAND ----------

query = """
    SELECT 
        id
        , agreement_id
        , delivery_week_type
        , start_date
        , status
    FROM register_info_basket
"""

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query)
