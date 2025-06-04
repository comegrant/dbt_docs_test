# Databricks notebook source
import sys
sys.path.append('../reusable')

# COMMAND ----------

from copy_data import create_or_replace_schemas

# COMMAND ----------

source_database = dbutils.widgets.get("source_database")
source_schema_prefix = dbutils.widgets.get("source_schema_prefix")
sink_schema_prefix = dbutils.widgets.get("sink_schema_prefix")
sink_database = spark.sql("SELECT current_catalog()").collect()[0][0]
schemas = [item.strip() for item in dbutils.widgets.get("schemas").split(",")]

# COMMAND ----------

create_or_replace_schemas(
    source_database = source_database, 
    sink_database = sink_database, 
    source_schema_prefix = source_schema_prefix,
    sink_schema_prefix = sink_schema_prefix,
    schemas = schemas,
    max_workers = 8
)
