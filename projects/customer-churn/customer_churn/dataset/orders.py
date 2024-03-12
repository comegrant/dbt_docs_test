import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


class Orders:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get orders data from database...")
        with Path.open(SQL_DIR / "orders.sql") as f:
            orders = self.db.read_data(f.read().format(company_id=self.company_id))

        return orders

    def load(self, reload_data: bool = False) -> pd.DataFrame:
        """Get orders data for company

        Args:
            company_id (str): company id
            db (DB): database connection

        Returns:
            pd.DataFrame: dataframe of orders data
        """
        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        self.df["delivery_date"] = pd.to_datetime(self.df.delivery_date)

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        df_snapshot = self.get_for_date(snapshot_date)
        if df_snapshot:
            return df_snapshot

        orders_features = df_snapshot[df_snapshot.delivery_date <= snapshot_date].copy()

        orders_features["year"] = snapshot_date.year
        orders_features["month"] = snapshot_date.month
        orders_features["week"] = snapshot_date.week

        df_labels = pd.DataFrame()
        if self.model_training:
            df_labels = (
                df_snapshot.loc[df_snapshot.delivery_date > snapshot_date]
                .groupby("agreement_id")
                .agg(number_of_forecast_orders=pd.NamedAgg(column="delivery_date", aggfunc="count"))
            )

        orders_features = orders_features.groupby("agreement_id").aggregate(
            max_delivery_date=pd.NamedAgg(column="delivery_date", aggfunc="max"),
            number_of_total_orders=pd.NamedAgg(column="delivery_date", aggfunc="count"),
            last_delivery_date=pd.NamedAgg(column="delivery_date", aggfunc="max"),
        )

        orders_features["weeks_since_last_delivery"] = (
            snapshot_date - orders_features["last_delivery_date"]
        ).dt.days / 7

        orders_features = orders_features.merge(df_labels, how="left", on="agreement_id")
        if self.model_training:
            orders_features["number_of_forecast_orders"] = orders_features["number_of_forecast_orders"].fillna(0)

        return orders_features[
            [
                "agreement_id",
                "snapshot_date",
                "year",
                "month",
                "week",
                "number_of_forecast_orders",
                "weeks_since_last_delivery",
                "number_of_total_orders",
            ]
        ]
