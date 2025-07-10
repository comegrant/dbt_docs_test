# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

# Name of source database
database = "operations"

# Name of table in source database
source_table = "orders_history"

# Name of date column to be used when finding most recent data in source
source_date_column = "created_date"

# Name of table in silver layer
silver_table = "base_operations__orders_history"

# Name of date column to be used when finding most recent data in silver
# Is often the created_at or updated_at column
silver_date_column = "source_created_at"

# Number of days since last ingest to extract data from
days = 14

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
query = f"""
    SELECT 
      ID
      ,ORDER_ID
      ,AGREEMENT_ID
      ,ORDER_STATUS_ID
      ,CREATED_BY
      ,CREATED_DATE
      ,MODIFIED_BY
      ,MODIFIED_DATE
      ,ROUTE_ID
      ,AGREEMENT_POSTALCODE
      ,WEEKDAY
      ,AGREEMENT_TIMEBLOCK
      ,[FROM]
      ,[TO]
      ,DELIVERY_DATE
      ,CITY
      ,WEEK_NR
      ,YEAR_NR
      ,INI_COST
      ,COMPANY_ID
      ,ZONE_ID
      ,TRANSPORT_COMPANY_ID
      ,COST1DROP
      ,COST2DROP
      ,TRANSPORT_NAME
      ,ORDER_ID_REFERENCE
      ,ORDER_TYPE
      ,POD_PLAN_COMPANY_ID
      ,LAST_MILE_HUB_DISTRIBUTION_CENTER_ID
      ,CONNECTED_HUB
      ,BAO_ID
      ,WHEN_TO_DETAIL_PLAN
      ,customer_fee
      ,country_id
      ,external_logistics_id
      ,logistics_drop_price
      ,logistics_other_cost
      ,logistic_system_id
      ,external_order_id
    FROM {source_table}
    WHERE {source_date_column} >= '{from_date}'
"""

# COMMAND ----------

# Extract data
load_coredb_query(dbutils, database, source_table, query)
