# Databricks notebook source
# COMMAND ----------
from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging

from ml_feature_store.common.spark_context import create_spark_context
from ml_feature_store.ft_weekly_dishes_variations.feature_table_builder import Args, build_feature_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# COMMAND ----------
env = dbutils.widgets.get("env")
is_drop_existing = dbutils.widgets.get("is_drop_existing").lower() == "true"
spark = create_spark_context()
args = Args(env=env, is_drop_existing=is_drop_existing)
build_feature_table(args=args, spark=spark)
