import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from customer_churn.paths import SQL_DIR

from .base import Dataset

logger = logging.getLogger(__name__)


class Orders(Dataset):
    def __init__(self, **kwargs: int):
        super().__init__(**kwargs)
        self.datetime_columns = ["delivery_date"]
        self.sql_file = "events.sql"
        self.entity_columns = ["agreement_id"]
        self.feature_columns = [
            "number_of_forecast_orders",
            "weeks_since_last_delivery",
            "number_of_total_orders",
        ]
        self.columns_out = self.entity_columns + self.feature_columns

        self.df = self.load()

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get orders data from database...")
        with Path.open(SQL_DIR / "orders.sql") as f:
            orders = self.db.read_data(f.read().format(company_id=self.company_id))

        return orders

    def load(self) -> pd.DataFrame:
        """Get orders data for company

        Args:
            company_id (str): company id
            db (DB): database connection

        Returns:
            pd.DataFrame: dataframe of orders data
        """
        return self.read_from_file() if self.input_file else self.read_from_db()

    def get_for_date(self, snapshot_date: datetime) -> pd.DataFrame:
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    def get_features_for_snapshot(self, snapshot_date: datetime) -> pd.DataFrame:
        df_snapshot = self.get_for_date(snapshot_date)
        if df_snapshot.empty:
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
                .agg(
                    number_of_forecast_orders=pd.NamedAgg(
                        column="delivery_date",
                        aggfunc="count",
                    ),
                )
            )

        orders_features = orders_features.groupby("agreement_id").aggregate(
            max_delivery_date=pd.NamedAgg(column="delivery_date", aggfunc="max"),
            number_of_total_orders=pd.NamedAgg(column="delivery_date", aggfunc="count"),
            last_delivery_date=pd.NamedAgg(column="delivery_date", aggfunc="max"),
        )

        orders_features["weeks_since_last_delivery"] = (
            snapshot_date - orders_features["last_delivery_date"]
        ).dt.days / 7

        if self.model_training:
            orders_features = orders_features.merge(
                df_labels,
                how="left",
                on="agreement_id",
            )
            orders_features["number_of_forecast_orders"] = orders_features[
                "number_of_forecast_orders"
            ].fillna(0)
        else:
            orders_features["number_of_forecast_orders"] = None

        return orders_features[self.feature_columns]
