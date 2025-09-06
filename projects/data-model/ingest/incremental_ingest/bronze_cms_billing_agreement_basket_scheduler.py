# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_table = "billing_agreement_basket_scheduler"
source_date_created_column = "created_at"
source_date_updated_column = "updated_at"

silver_table = f"silver.cms__{source_table}s"
silver_date_created_column = "source_created_at"
silver_date_updated_column = "source_updated_at"


# COMMAND ----------

# Define the widget
dbutils.widgets.text("incremental_load_days", "30")

# Number of days since last ingest to extract data from, based on parameter, else default to 30 days
days = int(dbutils.widgets.get("incremental_load_days"))

# COMMAND ----------

# From date is 30 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_date_created_column}), {days}) AS {days}d_before_max_date FROM {silver_table}")
    from_date = from_date_df.collect()[0][f'{days}d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

query_deviations = (
    f"""(
        SELECT * 
        FROM {source_table} 
        WHERE {source_date_created_column} >= '{from_date}'
        OR {source_date_updated_column} >= '{from_date}'
    )"""
)

# COMMAND ----------

load_coredb_query(dbutils, database, source_table, query_deviations)
