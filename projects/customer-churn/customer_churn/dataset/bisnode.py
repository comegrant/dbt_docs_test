import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


class Bisnode:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training
        self.feature_columns = [
            "agreement_id",
            "impulsiveness",
            "created_at",
            "cultural_class",
            "perceived_purchasing_power",
            "consumption",
            "children_probability",
            "financial_class",
            "life_stage",
            "type_of_housing",
            "confidence_level",
        ]

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get bisnode data from database...")
        with Path.open(SQL_DIR / "bisnode.sql") as f:
            bisnode = self.db.read_data(f.read().format(company_id=self.company_id))
        return bisnode

    def load(self, reload_data: bool = False) -> None:
        """Get bisnode data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """

        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        self.df["children_probability"] = self.df[["probability_children_0_to_6", "probability_children_7_to_17"]].max(
            axis=1,
        )
        self.df = self.df[self.feature_columns].dropna()

        for col in [
            "cultural_class",
            "perceived_purchasing_power",
            "financial_class",
            "life_stage",
            "type_of_housing",
            "confidence_level",
        ]:
            self.df[col] = self.df[col].str.replace(" ", "", regex=True).str.replace(".", "_", regex=True).str.lower()

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        """
        Returns rows for all customers for given date
        :param snapshot_date:
        :return: cleaned bisnode DataFrame
        """
        if self.df.empty:
            return self.df
        diff = self.df.created_at - pd.to_datetime(snapshot_date)
        df_min = diff.abs().groupby(self.df["agreement_id"]).idxmin()
        return self.df.loc[df_min]

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        snapshot_df = self.get_for_date(snapshot_date)

        diff = snapshot_df.created_at - pd.to_datetime(snapshot_date)
        df_min = diff.abs().groupby(snapshot_df["agreement_id"]).idxmin()
        return snapshot_df.loc[df_min]
