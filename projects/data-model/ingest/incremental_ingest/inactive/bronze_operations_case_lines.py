# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "OPERATIONS"
source_case_lines_table = "case_line"
source_case_lines_date_column = "case_line_date"

silver_case_lines_table = f"silver.operations__{source_case_lines_table}s"
silver_case_lines_date_column = "source_updated_at"

# Number of days since last ingest to extract data from
days = 60

# COMMAND ----------

# From date is 60 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_case_lines_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_case_lines_date_column}), {days}) AS {days}d_before_max_date FROM {silver_case_lines_table}")
    from_date = from_date_df.collect()[0][f'{days}d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

# Query to be sent to source database
query_case_lines = f"(SELECT * FROM {source_case_lines_table} WHERE {source_case_lines_date_column} >= '{from_date}')"

# COMMAND ----------

# Extract data
load_coredb_query(dbutils, database, source_case_lines_table, query_case_lines)

