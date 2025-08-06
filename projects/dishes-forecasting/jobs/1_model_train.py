# Databricks notebook source
from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------
from dishes_forecasting.train.run_train import Args, run_train

# COMMAND ----------
is_get_params_from_workflow = True
is_running_on_databricks = True
if is_running_on_databricks:
    os.environ["DATABRICKS_TOKEN"] = API_TOKEN
    os.environ["DATABRICKS_HOST"] = API_ROOT
    os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

# COMMAND ----------
# Getting parameters
if is_get_params_from_workflow:
    env = dbutils.widgets.get("env")
    company = dbutils.widgets.get("company")
else:
    env = "dev"
    company = "AMK"

# COMMAND ----------
args = Args(company=company, env=env, is_running_on_databricks=is_running_on_databricks)
run_train(args=args)
