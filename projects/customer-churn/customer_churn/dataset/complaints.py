import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


class Complaints:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get complaints data from database...")
        with Path.open(SQL_DIR / "complaints.sql") as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def load(self, reload_data: bool = False) -> None:
        """Get complaints data for company

        Args:
            company_id (str): company id
            db (DB): database connection

        Returns:
            pd.DataFrame: dataframe of complaints data
        """
        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        self.df.category = self.df.category.str.lower().replace(" ", "")
        self.df["delivery_date"] = pd.to_datetime(
            self.df.delivery_year * 1000 + self.df.delivery_week * 10 + 0,
            format="%Y%W%w",
        )

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        """
        Returns crm_segments for all customers for given date
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    def get_features_for_snapshot(self, snapshot_date: datetime, complaints_last_n_weeks: int = 4) -> pd.DataFrame:
        snapshot_df = self.get_for_date(snapshot_date)

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
            snapshot_df.delivery_date >= (snapshot_date + pd.DateOffset(weeks=-complaints_last_n_weeks))
        ]
        num_agreements_last_n_weeks = agreement_complaints_last_n_weeks.groupby("agreement_id").aggregate(
            number_of_complaints_last_n_weeks=pd.NamedAgg("agreement_id", "count"),
        )

        # Join together number of complaints last n weeks with the other features
        agreement_complaints = agreement_complaints.join(num_agreements_last_n_weeks, how="left").reset_index()
        agreement_complaints["number_of_complaints_last_n_weeks"] = (
            agreement_complaints["number_of_complaints_last_n_weeks"].fillna(0).astype(int)
        )

        return agreement_complaints[
            [
                "agreement_id",
                "total_complaints",
                "weeks_since_last_complaint",
                "number_of_complaints_last_n_weeks",
                "category",
            ]
        ]
