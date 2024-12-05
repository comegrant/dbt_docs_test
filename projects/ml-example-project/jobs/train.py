# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------
from ml_example_project.db import get_spark_session # noqa
from ml_example_project.train.train import Args

env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
is_run_on_databricks = dbutils.widgets.get("is_run_on_databricks")

spark = get_spark_session()
args = Args(
    company=company,
    env=env,
    is_run_on_databricks=is_run_on_databricks,
    is_use_feature_store=True
)

# COMMAND ----------
from ml_example_project.train.train import train_model  # noqa
train_model(
    args=args,
    spark=spark
)
