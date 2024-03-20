import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from customer_churn.paths import SQL_DIR

from .base import Dataset

logger = logging.getLogger(__name__)


class Customers(Dataset):
    def __init__(
        self,
        onboarding_weeks: int = 12,
        **kwargs: int,
    ):
        super().__init__(**kwargs)
        self.onboarding_weeks = onboarding_weeks
        self.datetime_columns = [
            "agreement_creation_date",
            "agreement_start_date",
            "agreement_first_delivery_date",
            "last_delivery_date",
            "next_estimated_delivery_date",
        ]

        self.entity_columns = ["agreement_id"]
        self.feature_columns = [
            "agreement_start_year",
            "agreement_start_week",
            "agreement_first_delivery_year",
            "agreement_first_delivery_week",
            "agreement_status",
            "agreement_start_date",
        ]
        self.columns_out = self.entity_columns + self.feature_columns

        self.df = self.load()

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get customers data from database...")
        with Path.open(SQL_DIR / "customers.sql") as f:
            df = self.db.read_data(f.read().format(company_id=self.company_id))
        return df

    def load(self) -> None:
        """Get customers data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """
        df = self.read_from_file() if self.file_exists() else self.read_from_db()

        customers_to_delete = df[df.agreement_status == "deleted"].shape[0]
        logger.info(
            "Total customers with delete status (will be ignored): "
            + str(customers_to_delete),
        )
        df = df.loc[df.agreement_status != "deleted"].copy()

        df["agreement_first_delivery_year"] = df.agreement_first_delivery_year.fillna(
            0,
        ).astype(int)
        df["agreement_first_delivery_week"] = df.agreement_first_delivery_week.fillna(
            0,
        ).astype(int)
        logger.info("Total Customers: " + str(df.shape[0]))

        return df

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        """
        Returns all customers older than snapshot_date + onboarding_weeks
        :param snapshot_date: Date from where the snapshot should be made (YYYY-MM-DD)
        :param onboarding_weeks:
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df
        dt_from = snapshot_date + pd.DateOffset(weeks=-self.onboarding_weeks)
        df = self.df.loc[self.df.agreement_start_date <= dt_from]
        return df[self.columns_out]

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        df = self.get_for_date(snapshot_date=snapshot_date).set_index("agreement_id")
        df["customer_since_weeks"] = (
            snapshot_date - df.agreement_start_date
        ).dt.days // 7

        return df
