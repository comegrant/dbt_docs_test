# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------
from dishes_forecasting.run_train import Args, run_train
from dishes_forecasting.spark_context import create_spark_context

spark = create_spark_context()

args = Args(company="GL", env="dev", is_running_on_databricks=True)
run_train(args=args, spark=spark)
