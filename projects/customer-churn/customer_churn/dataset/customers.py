import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


class Customers:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get customers data from database...")
        with Path.open(SQL_DIR / "customers.sql") as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))

        return df

    def load(self, reload_data: bool = False) -> None:
        """Get customers data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """
        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        customers_to_delete = self.df[self.df.agreement_status == "deleted"].shape[0]
        logger.info("Total customers with delete status (will be ignored): " + str(customers_to_delete))
        self.df = self.df.loc[self.df.agreement_status != "deleted"].copy()

        self.df["agreement_creation_date"] = pd.to_datetime(self.df.agreement_creation_date)
        self.df["agreement_start_date"] = pd.to_datetime(self.df.agreement_start_date)
        self.df["agreement_first_delivery_date"] = pd.to_datetime(self.df.agreement_first_delivery_date)
        self.df["last_delivery_date"] = pd.to_datetime(self.df.last_delivery_date)
        self.df["next_estimated_delivery_date"] = pd.to_datetime(self.df.next_estimated_delivery_date)
        self.df["agreement_first_delivery_year"] = self.df.agreement_first_delivery_year.fillna(0).astype(int)
        self.df["agreement_first_delivery_week"] = self.df.agreement_first_delivery_week.fillna(0).astype(int)
        logger.info("Total Customers: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date: datetime, onboarding_weeks: int = 12) -> pd.DataFrame:
        """
        Returns all customers older than snapshot_date + onboarding_weeks
        :param snapshot_date: Date from where the snapshot should be made (YYYY-MM-DD)
        :param onboarding_weeks:
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df
        dt_from = snapshot_date + pd.DateOffset(weeks=-onboarding_weeks)
        df = self.df.loc[self.df.agreement_start_date <= dt_from]
        return df[self.columns_out]

    def get_features_for_snapshot(self, snapshot_date: datetime, onboarding_weeks: int = 12) -> pd.DataFrame:
        return self.get_for_date(snapshot_date=snapshot_date, onboarding_weeks=onboarding_weeks)
