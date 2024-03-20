import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from customer_churn.paths import SQL_DIR

from .base import Dataset

logger = logging.getLogger(__name__)


class CRMSegments(Dataset):
    def __init__(self, **kwargs: int):
        super().__init__(**kwargs)
        self.entity_columns = ["agreement_id"]
        self.feature_columns = ["planned_delivery"]
        self.columns_out = self.entity_columns + self.feature_columns

        self.datetime_columns = []
        self.df = self.load()

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get crm segment data from database...")
        file_name = (
            "crm_segment_buyer.sql"
            if self.model_training
            else "crm_segment_deleted.sql"
        )

        with Path.open(SQL_DIR / file_name) as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def load(self) -> None:
        """Get crm segment data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """
        df = self.read_from_file() if self.file_exists() else self.read_from_db()
        df = df.replace(np.nan, "", regex=True)
        df["delivery_date"] = pd.to_datetime(
            df.current_delivery_year * 1000 + df.current_delivery_week * 10 + 0,
            format="%Y%W%w",
        )

        return df

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        """
        Returns crm_segments for all customers for given date
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df

        if self.model_training:
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
        return (
            snapshot_df.loc[date_filter]
            .sort_values(by="delivery_date", ascending=False)
            .groupby("agreement_id")
            .first()[self.feature_columns]
        )
