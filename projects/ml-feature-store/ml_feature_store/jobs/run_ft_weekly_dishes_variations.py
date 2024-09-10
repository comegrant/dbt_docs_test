# Databricks notebook source
# COMMAND ----------
import logging

from ml_feature_store.common.spark_context import create_spark_context
from ml_feature_store.ft_weekly_dishes_variations.build_features import Args, build_features

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

spark = create_spark_context()
args = Args(env="dev")
build_features(args=args, spark=spark)
