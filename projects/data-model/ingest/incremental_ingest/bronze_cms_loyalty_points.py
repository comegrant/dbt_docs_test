# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_table = "loyalty_points"
source_date_updated_column = "updated_at"

silver_table = f"silver.cms__{source_table}"
silver_date_updated_column = "source_updated_at"

# COMMAND ----------

# From date is 30 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_date_updated_column}), 30) AS 30d_before_max_date FROM {silver_table}")
    from_date = from_date_df.collect()[0]['30d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

query_deviations = (
    f"""(
        SELECT * 
        FROM {source_table} 
        WHERE {source_date_updated_column} >= '{from_date}'
    )"""
)

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query_deviations)
