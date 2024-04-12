import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from customer_churn.paths import SQL_DIR

from .base import Dataset

logger = logging.getLogger(__name__)


class Complaints(Dataset):
    def __init__(
        self,
        complaints_last_n_weeks: int = 4,
        **kwargs: int,
    ):
        super().__init__(**kwargs)
        self.datetime_columns = []
        self.feature_columns = [
            "total_complaints",
            "weeks_since_last_complaint",
            "number_of_complaints_last_n_weeks",
            "category",
        ]
        self.entity_columns = ["agreement_id"]
        self.columns_out = self.entity_columns + self.feature_columns
        self.complaints_last_n_weeks = complaints_last_n_weeks

        self.df = self.load()

    def get_default_values(self) -> dict:
        return {
            "weeks_since_last_complaint": -1,
            "total_complaints": -1,
            "last_n_complaints": -1,
            "number_of_complaints_last_n_weeks": -1,
            "category": "",
        }

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get complaints data from database...")
        with Path.open(SQL_DIR / "complaints.sql") as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def load(self) -> None:
        """Get crm segment data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """
        df = self.read_from_file() if self.file_exists() else self.read_from_db()

        df.category = df.category.str.lower().replace(" ", "")
        df["delivery_date"] = pd.to_datetime(
            df.delivery_year * 1000 + df.delivery_week * 10 + 0,
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
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        snapshot_df = self.get_for_date(snapshot_date)

        if snapshot_df.empty:
            return pd.DataFrame(columns=self.feature_columns).rename_axis(
                "agreement_id",
            )

        # Group by agreement and compute certain features
        agreement_complaints = snapshot_df.groupby("agreement_id").aggregate(
            total_complaints=pd.NamedAgg("agreement_id", "count"),
            last_complaint=pd.NamedAgg("delivery_date", "max"),
            category=pd.NamedAgg("category", "last"),
        )

        # Calculate difference from snapshot date and last complaint
        agreement_complaints["weeks_since_last_complaint"] = (
            snapshot_date - agreement_complaints["last_complaint"]
        ).dt.days // 7

        # Find number of complaints last n weeks
        agreement_complaints_last_n_weeks = snapshot_df[
            snapshot_df.delivery_date
            >= (snapshot_date + pd.DateOffset(weeks=-self.complaints_last_n_weeks))
        ]
        num_agreements_last_n_weeks = agreement_complaints_last_n_weeks.groupby(
            "agreement_id",
        ).aggregate(
            number_of_complaints_last_n_weeks=pd.NamedAgg("agreement_id", "count"),
        )

        # Join together number of complaints last n weeks with the other features
        agreement_complaints = agreement_complaints.join(
            num_agreements_last_n_weeks,
            how="left",
        )

        return agreement_complaints[self.feature_columns]
