# Databricks notebook source
from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------
import logging
import os
from typing import TYPE_CHECKING, Literal, cast

import mlflow
from reci_pick.predict.predict import Args, predict_recommendations

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""

# COMMAND ----------
from distutils.util import strtobool

API_ROOT = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------
dbutils.widgets.text("predict_date_iso_format", "")
dbutils.widgets.text("num_weeks", "4")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("company", "AMK")
dbutils.widgets.text("is_run_on_databricks", "True")
dbutils.widgets.text("is_from_workflow", "True")
dbutils.widgets.text("topk", "10")
# COMMAND ----------
env = cast(Literal["dev", "test", "prod"], dbutils.widgets.get("env"))
company = cast(Literal["LMK", "AMK", "GL", "RT"], dbutils.widgets.get("company"))
is_run_on_databricks = bool(strtobool(dbutils.widgets.get("is_run_on_databricks")))
is_from_workflow = bool(strtobool(dbutils.widgets.get("is_from_workflow")))
predict_date = dbutils.widgets.get("predict_date_iso_format")
num_weeks = int(dbutils.widgets.get("num_weeks"))
topk = int(dbutils.widgets.get("topk"))
# COMMAND ----------
args = Args(
    company=company,
    env=env,
    predict_date=predict_date,
    is_run_on_databricks=is_run_on_databricks,
    profile_name="sylvia-liu",
    is_from_workflow=is_from_workflow,
    num_weeks=num_weeks,
    topk=topk,
)

mlflow.set_registry_uri("databricks-uc")
if args.is_run_on_databricks:
    os.environ["DATABRICKS_TOKEN"] = API_TOKEN
    os.environ["DATABRICKS_HOST"] = API_ROOT
    os.environ["MLFLOW_USE_DATABRICKS_SDK_MODEL_ARTIFACTS_REPO_FOR_UC"] = "true"

try:
    logging.info(f"Starting prediction for company {args.company} in {args.env} environment")
    df_scores_list = predict_recommendations(args=args)
    logging.info(f"Successfully completed predictions, generated {len(df_scores_list)} result sets")
except Exception as e:
    logging.error(f"Prediction failed: {e}", exc_info=True)
    raise
