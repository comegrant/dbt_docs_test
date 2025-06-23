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
company = dbutils.widgets.get("company")
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
    df = await get_output_data(
        start_yyyyww=start_yyyyww, output_type=output_type, company=company, model_version=model_version
    )
    display(df)

    results_comp = compliancy_metrics(df)
    results_error = error_metrics(df)
    results_variation = await variation_metrics(df)
    summary = validation_summary(results_comp, results_error, results_variation)
    metrics = validation_metrics(results_comp, results_error, results_variation)

    total_records = metrics["sum_total_records"]
    error_compliancy = metrics["perc_broken_allergen"]
    warning_compliancy = metrics["perc_broken_preference"]
    error_mean_ordered_ago = metrics["perc_broken_mean_ordered_ago"]
    warning_avg_error = metrics["perc_broken_avg_error"]
    warning_acc_error = metrics["perc_broken_acc_error"]
    carb_warnings = metrics["perc_carb_warnings"]
    protein_warnings = metrics["perc_protein_warnings"]

    error = error_compliancy + error_mean_ordered_ago
    warning = warning_compliancy + warning_avg_error + warning_acc_error + carb_warnings + protein_warnings

    header_message = f"Preselector {company} Validation Finished"

    if model_version:
        header_message += f" for model version {model_version}"

    body_message_error = (
        f"⚠️ Errors Detected in Preselector Output!\n\n"
        f"Total number of instances: {total_records}\n"
        f"Errors:\n"
        f"- Allergen preference broken: {error_compliancy}% of instances.\n"
        f"- Mean ordered ago broken: {error_mean_ordered_ago}% of instances.\n"
        f"Warnings:\n"
        f"- Taste/concept preference broken: {warning_compliancy}% of instances.\n"
        f"- Aggregated error vector too high: {warning_avg_error}% of instances.\n"
        f"- Accumulated error vector too high: {warning_acc_error}% of instances.\n"
        f"- Carb variation too low: {carb_warnings}% of instances.\n"
        f"- Protein variation too low: {protein_warnings}% of instances.\n\n"
        f"See the run log for more information: {url_to_job} "
    )

    body_message_warning = (
        f"Warnings Detected in Preselector Output!\n"
        f"Total number of instances: {total_records}\n"
        f"Warnings:\n"
        f"- Taste/concept preference broken: {warning_compliancy}% of instances.\n"
        f"- Aggregated error vector too high: {warning_avg_error}% of instances.\n"
        f"- Accumulated error vector too high: {warning_acc_error}% of instances.\n"
        f"- Carb variation too low: {carb_warnings}% of instances.\n"
        f"- Protein variation too low: {protein_warnings}% of instances.\n\n"
        f"See the run log for more information: {url_to_job} "
    )

    if error > 0:
        logger.error("Validation of preselector output failed with errors")
        send_slack_notification(
            environment=env,
            header_message=header_message,
            body_message=body_message_error,
            relevant_people="mats",
            is_error=True,
        )

    if error == 0 and warning > 0:
        logger.warning("Validation of preselector output failed with warnings")
        send_slack_notification(
            environment=env,
            header_message=header_message,
            body_message=body_message_warning,
            relevant_people="mats",
            is_error=False,
        )

    logger.info(summary)


await run()
