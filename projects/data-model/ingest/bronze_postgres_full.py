# Databricks notebook source
from postgres_connector import load_postgres_full

# COMMAND ----------

load_postgres_full(dbutils, "net_backend", "ce_deviation_ordered")
load_postgres_full(dbutils, "net_backend_adams", "ce_deviation_ordered")
load_postgres_full(dbutils, "net_backend_linas", "ce_deviation_ordered")
load_postgres_full(dbutils, "net_backend_retnemt", "ce_deviation_ordered")

# COMMAND ----------

load_postgres_full(dbutils, "net_backend", "ce_preferences_updated")
load_postgres_full(dbutils, "net_backend_adams", "ce_preferences_updated")
load_postgres_full(dbutils, "net_backend_linas", "ce_preferences_updated")
load_postgres_full(dbutils, "net_backend_retnemt", "ce_preferences_updated")

# COMMAND ----------

load_postgres_full(dbutils, "net_backend", "update_subscription_step_completed")
load_postgres_full(dbutils, "net_backend_adams", "update_subscription_step_completed")
load_postgres_full(dbutils, "net_backend_linas", "update_subscription_step_completed")
load_postgres_full(dbutils, "net_backend_retnemt", "update_subscription_step_completed")

# COMMAND ----------

load_postgres_full(dbutils, "js", "update_subscription_step_viewed")
load_postgres_full(dbutils, "javascript_adams", "update_subscription_step_viewed")
load_postgres_full(dbutils, "javascript_lmk", "update_subscription_step_viewed")
load_postgres_full(dbutils, "javascript_retnemt", "update_subscription_step_viewed")
