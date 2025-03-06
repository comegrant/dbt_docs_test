# Databricks notebook source
# COMMAND ----------
import logging

from ml_feature_store.common.spark_context import create_spark_context
from ml_feature_store.ft_ml_recipes.feature_table_builder import Args, build_feature_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

spark = create_spark_context()
# get environment variable
env = dbutils.widgets.get("env")
is_drop_existing = dbutils.widgets.get("is_drop_existing").lower() == "true"
args = Args(env=env, is_drop_existing=is_drop_existing)
build_feature_table(args=args, spark=spark)
