# Databricks notebook source
import sys
import os
import logging

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Insert customer_churn to path
sys.path.insert(0, '../')

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

from pathlib import Path

from customer_churn.snapshot import RunArgs, generate_snapshot

# COMMAND ----------

generate_snapshot(
  RunArgs(
      company=COMPANY_CODE,
      start_date=start_date,
      end_date=end_date,
      save_snapshot=True,
      output_dir=Path("snapshot"),
      output_file_prefix="snapshot_",
      input_files=PREP_CONFIG["input_files"],
      snapshot_config=PREP_CONFIG["snapshot"],
      db_env="prod",
      db_connection_string="",
      local=False
  )
)

# COMMAND ----------



# COMMAND ----------


