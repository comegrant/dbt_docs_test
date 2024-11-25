# Databricks notebook source
# COMMAND ----------
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
import mlflow

# import mlflow.sklearn
from attribute_scoring.common import Args, get_spark_session
from attribute_scoring.train.train import train_pipeline
from databricks.feature_engineering import FeatureEngineeringClient

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Shared/ml_experiments/attribute-scoring")

env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
target = dbutils.widgets.get("target")

# COMMAND ----------
args = Args(company=company, env=env, target=target)
spark = get_spark_session()
fe = FeatureEngineeringClient()

train_pipeline(args=args, fe=fe, spark=spark)
