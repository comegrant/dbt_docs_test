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
from attribute_scoring.predict.predict import predict_pipeline
from databricks.feature_engineering import FeatureEngineeringClient

mlflow.set_tracking_uri("databricks")

env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
start_yyyyww_str = dbutils.widgets.get("start_yyyyww")
end_yyyyww_str = dbutils.widgets.get("end_yyyyww")

start_yyyyww = int(start_yyyyww_str) if start_yyyyww_str.lower() != "none" else None
end_yyyyww = int(end_yyyyww_str) if end_yyyyww_str.lower() != "none" else None
# COMMAND ----------
args = Args(company=company, env=env)
spark = get_spark_session()
fe = FeatureEngineeringClient()

predict_pipeline(args=args, fe=fe, spark=spark, start_yyyyww=start_yyyyww, end_yyyyww=end_yyyyww)
