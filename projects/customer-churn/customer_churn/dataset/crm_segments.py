import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


class CRMSegments:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training
        self.columns = ["agreement_id", "planned_delivery"]

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get crm segment data from database...")
        file_name = "crm_segment_buyer.sql" if self.model_training else "crm_segment_deleted.sql"

        with Path.open(SQL_DIR / file_name) as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def load(self, reload_data: bool = False) -> None:
        """Get crm segment data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """
        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        else:
            logger.info("Already read crm segment data from database")

    def get_for_date(self, snapshot_date: datetime, model_training: bool = False) -> pd.DataFrame:
        """
        Returns crm_segments for all customers for given date
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df

        if model_training:
            return self.df.loc[self.df.delivery_date <= snapshot_date].copy()

        return self.df.loc[self.df.delivery_date >= snapshot_date].copy()

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        snapshot_df = self.get_for_date(snapshot_date)

        if snapshot_df.empty:
            return snapshot_df

        date_filter = (
            snapshot_df.delivery_date <= snapshot_date
            if self.model_training
            else snapshot_df.delivery_date >= snapshot_date
        )
        return snapshot_df.loc[date_filter][self.columns]
