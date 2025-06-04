# Databricks notebook source
import sys
sys.path.append('../../reusable')

# COMMAND ----------

from copy_data import create_or_replace_tables

# COMMAND ----------

developer = dbutils.widgets.get("firstname_lastname")
layer = "silver"
dbt_schema = f"`~{developer}_{layer}`"
tables = dbutils.widgets.get("table_list")

# COMMAND ----------

create_or_replace_tables(
    source_database = "dev",
    source_schema = layer,
    sink_database = "dev",
    sink_schema = dbt_schema,
    tables = tables
)
