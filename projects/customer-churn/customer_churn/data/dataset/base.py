import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from data_connector import databricks_connector

from customer_churn.paths import DATA_DIR, SQL_DIR

logger = logging.getLogger(__name__)


class Dataset:
    def __init__(self, company_id: str, model_training: bool = False, input_file: str | None = None):
        self.company_id = company_id
        self.input_file = input_file
        self.model_training = model_training
        self.datetime_columns = None

    def get_default_values(self) -> dict:
        return {}

    def file_exists(self) -> bool:
        return self.input_file is not None and Path.exists(DATA_DIR / self.input_file)

    def read_from_file(self) -> pd.DataFrame:
        logger.info(f"Reading {self.input_file} data from file...")
        with Path.open(DATA_DIR / self.input_file) as f:
            df = pd.read_csv(f)
            for col in self.datetime_columns:
                df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")

        return df

    def read_from_db(self, filename: str) -> pd.DataFrame:
        logger.info(f"Get {filename} data from database...")
        with Path.open(SQL_DIR / filename) as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def read_from_databricks(self, filename: str, save_to_path: bool = False) -> pd.DataFrame:
        logger.info(f"Get {filename} data from databricks...")
        with Path.open(SQL_DIR / filename) as f:
            query = f.read().format(company_id=self.company_id)
            logger.info("Query: " + query)
            df = databricks_connector.read_data(query)
            for col in self.datetime_columns:
                df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")

        if save_to_path and not databricks_connector.is_running_databricks():
            df.to_csv(DATA_DIR / "{}.csv".format(filename.split(".")[0]), index=False)

        return df

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        raise NotImplementedError
