# Databricks notebook source
import logging
import sys

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Insert customer_churn to path
sys.path.insert(0, "../")

# Insert lmk_utils package to path
sys.path.insert(0, "../../../packages/lmkgroup-ds-utils/")

# Insert package pydantic_argparser to path
sys.path.insert(0, "../../../packages/pydantic-argparser/")


# COMMAND ----------

try:
    COMPANY_CODE = str(getArgument("company"))
except:
    logger.warning("Failed to read company id from argument, using default")
    COMPANY_CODE = "RN"

# COMMAND ----------


# COMMAND ----------


# COMMAND ----------
