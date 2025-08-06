# Databricks notebook source
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------
from dishes_forecasting.train.run_tune import Args, run_tune

# COMMAND ----------
is_get_params_from_workflow = True
is_running_on_databricks = True
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
run_tune(args=args)
