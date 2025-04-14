# Databricks notebook source
from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os
from typing import TYPE_CHECKING, Literal, cast

import mlflow
from reci_pick.train.train import Args, train_model

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore
# COMMAND ----------
from distutils.util import strtobool

API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("company", "AMK")
dbutils.widgets.text("is_run_on_databricks", "True")
dbutils.widgets.text("is_from_workflow", "True")
dbutils.widgets.text("is_register_model", "True")
# COMMAND ----------
env = cast(Literal["dev", "test", "prod"], dbutils.widgets.get("env"))
company = cast(Literal["LMK", "AMK", "GL", "RT"], dbutils.widgets.get("company"))
is_run_on_databricks = bool(strtobool(dbutils.widgets.get("is_run_on_databricks")))
is_from_workflow = bool(strtobool(dbutils.widgets.get("is_from_workflow")))
is_register_model = bool(strtobool(dbutils.widgets.get("is_register_model")))

# COMMAND ----------
args = Args(
    company=company,
    env=env,
    is_run_on_databricks=is_run_on_databricks,
    is_register_model=is_register_model,
    is_from_workflow=is_from_workflow,
)

if args.is_run_on_databricks:
    os.environ["DATABRICKS_TOKEN"] = API_TOKEN
    os.environ["DATABRICKS_HOST"] = API_ROOT
    os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"
    mlflow.set_tracking_uri("databricks")

else:
    mlflow.set_tracking_uri(f"databricks://{args.profile_name}")
train_model(args=args)
