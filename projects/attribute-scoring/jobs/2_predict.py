# Databricks notebook source
# COMMAND ----------
import logging
import os
from typing import TYPE_CHECKING

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""

# COMMAND ----------
API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_TOKEN"] = API_TOKEN
os.environ["DATABRICKS_HOST"] = API_ROOT
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

# COMMAND ----------
import mlflow

# import mlflow.sklearn
from attribute_scoring.common import Args
from attribute_scoring.db import get_spark_session
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
