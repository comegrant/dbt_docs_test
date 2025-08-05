# Databricks notebook source
import sys
sys.path.append('../reusable')

# COMMAND ----------

from copy_data import create_or_replace_objects

# COMMAND ----------

source_database = dbutils.widgets.get("source_database")
source_schema = dbutils.widgets.get("source_schema")
sink_schema = dbutils.widgets.get("sink_schema")
sink_database = spark.sql("SELECT current_catalog()").collect()[0][0]
tables = [item.strip() for item in dbutils.widgets.get("tables").split(",")]

# COMMAND ----------

create_or_replace_objects(
    source_database = source_database,
    source_schema = source_schema,
    sink_database = sink_database,
    sink_schema = sink_schema,
    objects = tables
)
