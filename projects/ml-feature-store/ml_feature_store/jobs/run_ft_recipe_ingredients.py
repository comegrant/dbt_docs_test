# Databricks notebook source
# COMMAND ----------
from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging

from ml_feature_store.common.spark_context import create_spark_context
from ml_feature_store.ft_recipe_ingredients.feature_table_builder import Args, build_feature_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------
spark = create_spark_context()
env = dbutils.widgets.get("env")
args = Args(env=env)
build_feature_table(args=args, spark=spark)
