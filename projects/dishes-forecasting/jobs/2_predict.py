# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
import os  # noqa: 402

API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_TOKEN"] = API_TOKEN
os.environ["DATABRICKS_HOST"] = API_ROOT
os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"
# COMMAND ----------
import mlflow  # noqa: 402
from dishes_forecasting.predict.run_predict import Args, run_predict  # noqa: 402
from distutils.util import strtobool

is_running_on_databricks = True
# COMMAND ----------
# Getting parameters

dbutils.widgets.text(name="write_mode", defaultValue="append")
dbutils.widgets.text(name="env", defaultValue="dev")
dbutils.widgets.text(name="company", defaultValue="GL")
dbutils.widgets.text(name="new_portions", defaultValue="5")  # must be a comma-separated list of integers
dbutils.widgets.text(name="new_portions_targets", defaultValue="0.05")  # must be a comma-separated list of integers
dbutils.widgets.text(name="is_modify_new_portions", defaultValue="true")
dbutils.widgets.text(name="is_add_new_portions_on_top", defaultValue="false")
dbutils.widgets.text(name="is_normalize_predictions", defaultValue="true")

# COMMAND ----------
env = dbutils.widgets.get("env")
company = dbutils.widgets.get("company")
write_mode = dbutils.widgets.get("write_mode")
new_portions = [int(i) for i in dbutils.widgets.get("new_portions").split(",")]
new_portions_targets = [float(i) for i in dbutils.widgets.get("new_portions_targets").split(",")]
is_modify_new_portions = bool(strtobool(dbutils.widgets.get("is_modify_new_portions")))
is_add_new_portions_on_top = bool(strtobool(dbutils.widgets.get("is_add_new_portions_on_top")))
is_normalize_predictions = bool(strtobool(dbutils.widgets.get("is_normalize_predictions")))

# COMMAND ----------
mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")

args = Args(
    company=company,
    env=env,
    forecast_date="",
    is_running_on_databricks=is_running_on_databricks,
    is_normalize_predictions=is_normalize_predictions,
    is_modify_new_portions=is_modify_new_portions,
    is_add_new_portions_on_top=is_add_new_portions_on_top,
    new_portions=new_portions,
    new_portions_targets=new_portions_targets,
)

print(args)  # noqa: T201

# COMMAND ----------
df_predictions = run_predict(
    args=args,
)

display(df_predictions)
