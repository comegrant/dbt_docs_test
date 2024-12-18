# Databricks notebook source

# COMMAND ----------

from databricks_env import auto_setup_env

auto_setup_env()
# COMMAND ----------
import logging
import os

from data_contracts.sources import databricks_config
from preselector.output_validation import (
    compliancy_metrics,
    get_output_data,
    validation_metrics,
    validation_summary,
)
from slack_connector.slack_notification import send_slack_notification

# COMMAND ----------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_yyyyww_str = dbutils.widgets.get("start_yyyyww")
start_yyyyww = int(start_yyyyww_str) if start_yyyyww_str.lower() != "none" else None
output_type = dbutils.widgets.get("output_type")  # batch or realtime
env = dbutils.widgets.get("env")

try:
    spark = databricks_config.connection()
except Exception as e:
    logger.error(f"Failed to create Spark session: {e}")
    raise e

os.environ["UC_ENV"] = env


async def run() -> None:
    df = await get_output_data(start_yyyyww, output_type)

    results = compliancy_metrics(df)
    summary = validation_summary(results)
    total_checks, warnings, errors = validation_metrics(results)

    header_message = "Preselector Validation Finished"

    body_message_error = (
        f"❌ Errors Detected in Preselector Output:\n"
        f"- {errors} out of {total_checks} datasets have broken allergens.\n"
        f"- {warnings} out of {total_checks} datasets have broken preferences.\n\n"
        f"See the run log for more information. "
    )

    body_message_warning = (
        f"Warnings Detected in Preselector Output:\n"
        f"- {warnings} out of {total_checks} datasets have broken preferences.\n\n"
        f"See the run log for more information. "
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

    if errors == 0 and warnings > 0:
        logger.warning(f"Validation of preselector output failed with {warnings} warnings")
        send_slack_notification(
            environment=env,
            header_message=header_message,
            body_message=body_message_warning,
            relevant_people="mats, niladri",
            is_error=False,
        )

    logger.info(summary)


await run()
