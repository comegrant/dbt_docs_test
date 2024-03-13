import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from customer_churn.paths import SQL_DIR

from .base import Dataset

logger = logging.getLogger(__name__)


class Bisnode(Dataset):
    def __init__(self, **kwargs: int):
        super().__init__(**kwargs)
        self.datetime_columns = ["created_at"]
        self.entity_columns = ["agreement_id"]
        self.feature_columns = [
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
        self.columns_out = self.entity_columns + self.feature_columns

        self.df = self.load()

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get bisnode data from database...")
        with Path.open(SQL_DIR / "bisnode.sql") as f:
            bisnode = self.db.read_data(f.read().format(company_id=self.company_id))
        return bisnode

    def load(self) -> None:
        """Get bisnode data for company

        Args:
            company_id (str): company id
            db (DB): database connection
        """

        df = self.read_from_file() if self.input_file else self.read_from_db()

        df["children_probability"] = df[
            ["probability_children_0_to_6", "probability_children_7_to_17"]
        ].max(
            axis=1,
        )
        df = df[self.columns_out].dropna()

        for col in [
            "cultural_class",
            "perceived_purchasing_power",
            "financial_class",
            "life_stage",
            "type_of_housing",
            "confidence_level",
        ]:
            df[col] = (
                df[col]
                .str.replace(" ", "", regex=True)
                .str.replace(".", "_", regex=True)
                .str.lower()
            )

        return df

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

        if snapshot_df.empty:
            return snapshot_df

        diff = snapshot_df.created_at - pd.to_datetime(snapshot_date)
        df_min = diff.abs().groupby(snapshot_df["agreement_id"]).idxmin()
        return snapshot_df.loc[df_min]
