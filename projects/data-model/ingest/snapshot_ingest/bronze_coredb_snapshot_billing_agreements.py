# Databricks notebook source
# MAGIC %md
# MAGIC Used to perform additional ingest of billing_agreements outside of the snapshot_full notebook

# COMMAND ----------
import sys
sys.path.append('../helper_functions')

from coredb_connector import load_coredb_full

# COMMAND ----------

load_coredb_full(dbutils, "CMS", "billing_agreement")
