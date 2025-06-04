# Databricks notebook source
# MAGIC %md
# MAGIC This is not a part of the full cms ingest because we avoid including personal info.

# COMMAND ----------
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "operations"
source_table = "orders"

# COMMAND ----------

query = f"""
    SELECT 
      ORDER_ID
      ,AGREEMENT_ID
      ,AGREEMENT_POSTALCODE
      ,AGREEMENT_TIMEBLOCK
      ,COMPANY_ID
      ,ORDER_STATUS_ID
      ,ORDER_ID_REFERENCE
      ,WEEKDATE
      ,CREATED_BY
      ,CREATED_DATE
      ,MODIFIED_BY
      ,MODIFIED_DATE
      ,WEEK_NR
      ,YEAR_NR
      ,ORDER_TYPE
      ,BAO_ID
      ,POD_PLAN_COMPANY_ID
      ,country_id
      ,external_logistics_id
      ,logistic_system_id
      ,has_recipe_leaflets
      ,external_order_id
      ,period_nr
    FROM {source_table}
"""

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query)
