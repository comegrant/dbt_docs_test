# Databricks notebook source
# COMMAND ----------

from databricks_env import auto_setup_env

auto_setup_env()

# COMMAND ----------

import logging
from datetime import date, timedelta

from data_contracts.recommendations.store import recommendation_feature_contracts

# COMMAND ----------

# Only log errors from the azure module.
# It can create a log of noise when connecting to the data lake
logging.basicConfig(level=logging.INFO)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
    logging.ERROR
)

# COMMAND ----------

store = recommendation_feature_contracts()

# COMMAND ----------

# Setting default values so we have a rolling computation window
# However, it is also possible to define a non-rolling window for consistency / debugging purposes

today = date.today()

default_weeks_to_predict = 7

dbutils.widgets.text("company_id", "09ECD4F0-AE58-4539-8E8F-9275B1859A19")
dbutils.widgets.text("train_to_date_iso_format", today.isoformat())
dbutils.widgets.text("weeks_to_train_on", "36")
dbutils.widgets.text(
    "train_from_date_iso_format", (today - timedelta(weeks=36)).isoformat()
)
dbutils.widgets.text(
    "predict_from_iso_format", (today + timedelta(weeks=1)).isoformat()
)
dbutils.widgets.text("number_of_weeks_to_predict", f"{default_weeks_to_predict}")
dbutils.widgets.text(
    "predict_to_iso_format",
    (today + timedelta(weeks=1 + default_weeks_to_predict)).isoformat(),
)

company_id = dbutils.widgets.get("company_id")

train_to_date_iso_format = dbutils.widgets.get("train_to_date_iso_format")
train_to_date = date.fromisoformat(train_to_date_iso_format)

weeks_to_train_on = int(dbutils.widgets.get("weeks_to_train_on"))
default_train_from = train_to_date - timedelta(weeks=weeks_to_train_on)

train_from_date_iso_format = dbutils.widgets.get("train_from_date_iso_format")
train_from_date = date.fromisoformat(train_from_date_iso_format)

predict_from_iso_format = dbutils.widgets.get("predict_from_iso_format")
predict_from = date.fromisoformat(predict_from_iso_format)

number_of_weeks_to_predict = int(dbutils.widgets.get("number_of_weeks_to_predict"))

default_predict_to_date = predict_from + timedelta(weeks=number_of_weeks_to_predict)
predict_to_iso_format = dbutils.widgets.get("predict_to_iso_format")
predict_to_date = date.fromisoformat(predict_to_iso_format)


def year_weeks_between_dates(start_date: date, end_date: date) -> list[int]:
    if start_date > end_date:
        raise ValueError("Start date must be before end date")

    return [
        int((start_date + timedelta(weeks=i)).strftime("%Y%W"))
        for i in range((end_date - start_date).days // 7)
    ]


# COMMAND ----------
import os

os.environ["DATALAKE_SERVICE_ACCOUNT_NAME"] = dbutils.secrets.get(
    scope="auth_common",
    key="service-account-name-experimental",
)
os.environ["DATALAKE_STORAGE_ACCOUNT_KEY"] = dbutils.secrets.get(
    scope="auth_common",
    key="storage-account-key-experimental",
)

# COMMAND ----------

from rec_engine.run import CompanyDataset, run

dataset = CompanyDataset(
    company_id=company_id,
    year_weeks_to_train_on=year_weeks_between_dates(train_from_date, train_to_date),
    year_weeks_to_predict_on=year_weeks_between_dates(predict_from, predict_to_date),
)

# COMMAND ----------

await run(
    dataset=dataset, store=store, write_to_path=None
)  # None will write to prod source
