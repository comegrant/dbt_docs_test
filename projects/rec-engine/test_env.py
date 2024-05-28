# Databricks notebook source
# COMMAND ----------

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------

from data_contracts.recommendations.store import recommendation_feature_contracts

store = recommendation_feature_contracts()

print(store)
