# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

# Name of source database
database = "operations"

# Name of table in source database
source_table = "order_collis_history"

# Name of date column to be used when finding most recent data in source
source_date_column = "created_at"

# Name of table in silver layer
silver_table = "base_operations__order_collis_history"

# Name of date column to be used when finding most recent data in silver
# Is often the created_at or updated_at column
silver_date_column = "source_created_at"

# Number of days since last ingest to extract data from
days = 30

# COMMAND ----------

# Find the date X days before max date of corresponding silver table
try: 
    spark.table(f"{silver_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_date_column}), {days}) AS {days}d_before_max_date FROM {silver_table}")
    from_date = from_date_df.collect()[0][f'{days}d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

# Query to be sent to source database
query = f"(SELECT * FROM {source_table} WHERE {source_date_column} >= '{from_date}')"

# COMMAND ----------

# Extract data
load_coredb_query(dbutils, database, source_table, query)