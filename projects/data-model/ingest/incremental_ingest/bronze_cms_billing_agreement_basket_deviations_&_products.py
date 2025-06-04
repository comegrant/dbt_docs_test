# Databricks notebook source
import sys
sys.path.append('../../reusable')

from datetime import datetime

from coredb_connector import load_coredb_query

# COMMAND ----------

database = "CMS"
source_deviations_table = "billing_agreement_basket_deviation"
source_deviations_date_column = "created_at"
source_deviations_join_column = "id"

silver_deviations_table = f"silver.cms__{source_deviations_table}s"
silver_deviations_date_column = "source_created_at"

source_deviation_products_table = "billing_agreement_basket_deviation_product"
source_deviation_products_join_column = "billing_agreement_basket_deviation_id"

# COMMAND ----------

# From date is 60 days before max date of corresponding silver table
try: 
    spark.table(f"{silver_deviations_table}")
    from_date_df = spark.sql(f"SELECT DATE_SUB(MAX({silver_deviations_date_column}), 60) AS 45d_before_max_date FROM {silver_deviations_table}")
    from_date = from_date_df.collect()[0]['45d_before_max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

# COMMAND ----------

query_deviations = f"(SELECT * FROM {source_deviations_table} WHERE {source_deviations_date_column} >= '{from_date}')"

# COMMAND ----------

load_coredb_query(dbutils, database, source_deviations_table, query_deviations)

# COMMAND ----------

# Inner join to find the order lines that belong to the deviations
query_deviation_products = (
f"""(
    SELECT deviation_products.* 
    FROM {source_deviations_table} as deviations
    INNER JOIN {source_deviation_products_table} as deviation_products 
    ON deviations.{source_deviations_join_column} = deviation_products.{source_deviation_products_join_column} 
    WHERE deviations.{source_deviations_date_column} >= '{from_date}'
)"""
)

# COMMAND ----------

load_coredb_query(dbutils, database, source_deviation_products_table, query_deviation_products)
