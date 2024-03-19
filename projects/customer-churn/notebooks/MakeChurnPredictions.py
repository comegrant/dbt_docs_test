# Databricks notebook source
from pip._internal.operations import freeze
import sys

packages = list(freeze.freeze())
editable_packages = [
  package for package in packages if package.startswith("# Editable")
]
local_paths = [
  "/Workspace" + package.split("/Workspace")[-1]
  for package in editable_packages
]
sys.path.extend(local_paths)

# COMMAND ----------

import logging
import pandas as pd
import numpy as np
from datetime import datetime


from lmkgroup_ds_utils.db.connector import DB
from lmkgroup_ds_utils.constants import Company

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

from customer_churn.predict import run_predict

# COMMAND ----------

from customer_churn.model_logreg import ModelLogReg

# COMMAND ----------

## Arguments that must be Changed

COMPANY = "RN" # str(getArgument("company"))

PREP_CONFIG = {
    "snapshot": {
        'start_date': '2021-06-01',  # Data snapshot start date
        'end_date': datetime.now().strftime('%Y-%m-%d'),  # Data snapshot end date
        'forecast_weeks': 4,  # Prediction n-weeks ahead
        'buy_history_churn_weeks': 4  # Number of customer weeks to include in data model history
    },
    "output_dir": "/processed/"  # Output directory
}

ALL_COMPANY_ID = {
  "LMK": {
    'id': Company.LMK,
    'schema':'javascript_lmk'
  },
  'GL': {
    'id': Company.GL,
    'schema':'js'
  }, 
  'AMK': {
    'id': Company.AMK,
    'schema':'javascript_adams'
  },
  'RN': {
    'id': Company.RN,
    'schema':'javascript_retnemt'
  }
}

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

model_name = COMPANY + "_CHURN"

model = ModelLogReg(config=PREP_CONFIG)
model_obj = mlflow.sklearn.load_model(f"models:/" + model_name + "/production")
pred = model.predict(df, model_obj=model_obj)
