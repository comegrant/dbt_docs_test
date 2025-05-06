# Databricks notebook source
import sys
sys.path.append('../helper_functions')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "OPERATIONS"
source_cases_table = "cases"
source_cases_date_column = "case_status_last_change"

silver_cases_table = f"silver.operations__{source_cases_table}"
silver_cases_date_column = "source_updated_at"

# Number of days since last ingest to extract data from
days = 60

# COMMAND ----------

# From date is 60 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_cases_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_cases_date_column}), {days}) AS {days}d_before_max_date FROM {silver_cases_table}")
    from_date = from_date_df.collect()[0][f'{days}d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

# Query to be sent to source database
query_cases = f"(SELECT * FROM {source_cases_table} WHERE {source_cases_date_column} >= '{from_date}')"

# COMMAND ----------

# Extract data
load_coredb_query(dbutils, database, source_cases_table, query_cases)