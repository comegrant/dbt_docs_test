# Databricks notebook source
import sys
sys.path.append('../../../packages/data-connector')

# COMMAND ----------

from datetime import datetime, timedelta

from data_connector.coredb_connector import load_coredb_full

# COMMAND ----------

database = "CMS"
table = "campaign_events"
date_column = "updated_at"

# COMMAND ----------

#From date is max date of corresponding silver table
try: 
    spark.table(f"silver.cms_{table}")
    from_date_df = spark.sql(f"SELECT MAX({date_column}) AS max_date FROM silver.cms_{table}")
    from_date = from_date_df.collect()[0]['max_date'].strftime('%Y-%m-%d')
except Exception as e:
    from_date = '2015-01-01'

to_date = datetime.today().strftime('%Y-%m-%d')


# COMMAND ----------

query = f"(SELECT * FROM {table} WHERE {date_column} BETWEEN '{from_date}' AND '{to_date}')"

# COMMAND ----------

load_coredb_query(database, table, query)
