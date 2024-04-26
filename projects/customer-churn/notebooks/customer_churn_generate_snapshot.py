# Databricks notebook source
import sys
import logging
from datetime import datetime, UTC, timedelta

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

packages = [
    "../",
    "../../../packages/data-connector/",
    "../../../packages/pydantic-argparser/",
    "../../../packages/constants/",
]

sys.path.extend(packages)


# COMMAND ----------

try:
    COMPANY_CODE = str(getArgument("company"))
except:
    logger.warning("Failed to read company id from argument, using default")
    COMPANY_CODE = "RT"

# COMMAND ----------

from customer_churn.snapshot import RunArgs, generate_snapshot

# COMMAND ----------

generate_snapshot(
    RunArgs(
        company=COMPANY_CODE,
        start_date=datetime.now(tz=UTC).date() - timedelta(days=360),
        end_date=datetime.now(tz=UTC).date(),
        save_snapshot=True,
    )
)
