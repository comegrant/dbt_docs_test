# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_orders_table = "billing_agreement_order"
source_orders_date_column = "created_date"
source_orders_join_column = "id"

silver_orders_table = f"silver.cms__{source_orders_table}s"
silver_orders_date_column = "source_created_at"

source_order_lines_table = "billing_agreement_order_line"
source_order_lines_join_column = "agreement_order_id"

# COMMAND ----------

# From date is 30 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_orders_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_orders_date_column}), 30) AS 30d_before_max_date FROM {silver_orders_table}")
    from_date = from_date_df.collect()[0]['30d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

query_orders = f"(SELECT * FROM {source_orders_table} WHERE {source_orders_date_column} >= '{from_date}')"

# COMMAND ----------

load_coredb_query(dbutils, database, source_orders_table, query_orders)

# COMMAND ----------

# Inner join to find the order lines that belong to the orders
query_order_lines = (
f"""(
    SELECT order_lines.* 
    FROM {source_orders_table} as orders
    INNER JOIN {source_order_lines_table} as order_lines 
    ON orders.{source_orders_join_column} = order_lines.{source_order_lines_join_column} 
    WHERE orders.{source_orders_date_column} >= '{from_date}'
)"""
)

# COMMAND ----------

load_coredb_query(dbutils, database, source_order_lines_table, query_order_lines)
