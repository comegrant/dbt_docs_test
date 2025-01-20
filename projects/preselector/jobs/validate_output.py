# Databricks notebook source

# COMMAND ----------
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""  # type: ignore

from databricks_env import auto_setup_env

auto_setup_env()
# COMMAND ----------
import logging
import os

from data_contracts.sources import databricks_config
from preselector.output_validation import (
    compliancy_metrics,
    error_metrics,
    get_output_data,
    validation_metrics,
    validation_summary,
    variation_metrics,
)
from slack_connector.slack_notification import send_slack_notification

# COMMAND ----------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_yyyyww_str = dbutils.widgets.get("start_yyyyww")
start_yyyyww = int(start_yyyyww_str) if start_yyyyww_str.lower() != "none" else None
output_type = dbutils.widgets.get("output_type")  # batch or realtime
env = dbutils.widgets.get("env")
model_version_str = dbutils.widgets.get("model_version")
model_version = model_version_str if model_version_str.lower() != "none" else None


try:
    spark = databricks_config.connection()
except Exception as e:
    logger.error(f"Failed to create Spark session: {e}")
    raise e

os.environ["UC_ENV"] = env
os.environ["DATALAKE_ENV"] = env

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-name",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="azure-storageAccount-experimental-key",
)
# COMMAND ----------
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

job_id = spark.sparkContext.getLocalProperty("spark.databricks.job.id")  # Gives JobID in the URL
parent_run_id = spark.sparkContext.getLocalProperty("spark.databricks.job.parentRunId")  # Gives Job RunID in the URL

DATABRICKS_HOSTS = {
    "dev": "https://adb-4291784437205825.5.azuredatabricks.net",
    "test": "https://adb-3194696976104051.11.azuredatabricks.net",
    "prod": "https://adb-3181126873992963.3.azuredatabricks.net",
}

url_to_job = f"{DATABRICKS_HOSTS[env]}/jobs/{job_id}/runs/{parent_run_id}"


# COMMAND ----------


async def run() -> None:
    df = await get_output_data(start_yyyyww=start_yyyyww, output_type=output_type, model_version=model_version)
    display(df)

    results_comp = compliancy_metrics(df)
    results_error = error_metrics(df)
    results_variation = await variation_metrics(df)
    summary = validation_summary(results_comp, results_error, results_variation)
    metrics = validation_metrics(results_comp, results_error, results_variation)

    total_checks = metrics["total_checks"]
    errors_compliancy = metrics["comliancy_errors"]
    warnings_compliancy = metrics["comliancy_warnings"]
    errors_error = metrics["vector_errors"]
    warnings_error = metrics["vector_warnings"]
    carb_warnings = metrics["carb_warnings"]
    protein_warnings = metrics["protein_warnings"]

    errors = errors_compliancy + errors_error
    warnings = warnings_compliancy + warnings_error
    total_warnings = warnings + carb_warnings + protein_warnings

    header_message = "Preselector Validation Finished"

    body_message_error = (
        f"❌ Errors Detected in Preselector Output:\n"
        f"Errors:\n"
        f"- {errors_compliancy} out of {total_checks} datasets have broken allergens.\n"
        f"- {errors_error} out of {total_checks} datasets have broken mean ordered ago.\n"
        f"Warnings:\n"
        f"- {warnings_compliancy} out of {total_checks} datasets have broken preferences.\n"
        f"- {warnings_error} out of {total_checks} datasets have broken aggregated errors.\n"
        f"- {carb_warnings} out of {total_checks} datasets have too little carb variation.\n"
        f"- {protein_warnings} out of {total_checks} datasets have too little protein variation.\n\n"
        f"See the run log for more information: {url_to_job} "
    )

    body_message_warning = (
        f"Warnings Detected in Preselector Output:\n"
        f"Warnings:\n"
        f"- {warnings_compliancy} out of {total_checks} datasets have broken preferences.\n"
        f"- {warnings_error} out of {total_checks} datasets have broken aggregated error.\n"
        f"- {carb_warnings} out of {total_checks} datasets have too little carb variation.\n"
        f"- {protein_warnings} out of {total_checks} datasets have too little protein variation.\n\n"
        f"See the run log for more information: {url_to_job} "
    )

    if errors > 0:
        logger.error(f"Validation of preselector output failed with {errors} errors")
        send_slack_notification(
            environment=env,
            header_message=header_message,
            body_message=body_message_error,
            relevant_people="mats, niladri",
            is_error=True,
        )

    if errors == 0 and total_warnings > 0:
        logger.warning(f"Validation of preselector output failed with {total_warnings} warnings")
        send_slack_notification(
            environment=env,
            header_message=header_message,
            body_message=body_message_warning,
            relevant_people="mats, niladri",
            is_error=False,
        )

    logger.info(summary)


await run()
