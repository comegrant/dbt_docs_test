# Holds the dataprep logic.

import logging
from datetime import datetime
from functools import reduce

import numpy as np
import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.config import PREP_CONFIG
from customer_churn.constants import LABEL_TEXT_CHURNED, LABEL_TEXT_DELETED
from customer_churn.data.dataset.complaints import Complaints
from customer_churn.data.dataset.crm_segments import CRMSegments
from customer_churn.data.dataset.customers import Customers
from customer_churn.data.dataset.events import Events
from customer_churn.data.dataset.orders import Orders

logger = logging.getLogger(__name__)


class DataLoader:
    """
    INFO: Main method for generating datasets for training, evaluation and estimation
    """

    def __init__(
        self,
        company_id: str,
        model_training: bool = False,
        input_files: dict = PREP_CONFIG["input_files"],
        snapshot_config: dict = PREP_CONFIG["snapshot"],
        adb: DB = None,
        postgres_db: DB = None,
    ) -> None:
        """
        :param config: config_input_files i.e. just the path to the source files but not the content
        """
        self.adb = adb
        self.postgres_db = postgres_db
        self.datasets = self._load_data(
            company_id,
            model_training,
            snapshot_config,
            input_files,
        )
        self.buy_history_churn_weeks = snapshot_config.get(
            "buy_history_churn_weeks",
            4,
        )
        self.model_training = model_training

    def generate_features_for_date(
        self,
        snapshot_date: str,
        customer_id: str | None = None,
    ) -> None | pd.DataFrame:
        """
        :param customer: Customer DataFrame (single customer)
        :param snapshot_date: snapshot data
        :return: None or Merged snapshot from events, orders and customer dataset for given snapshot date
        """

        if customer_id:
            raise NotImplementedError(
                "Features for single customer not implemented yet",
            )

        snapshot_features = self._get_features(snapshot_date=snapshot_date)
        snapshot_features_with_labels = self._add_labels(df=snapshot_features)

        return snapshot_features_with_labels

    def generate_features_for_customer(self, customer_id: int) -> pd.DataFrame:
        logger.info(f"Generating features for single customer {customer_id}")

        _snapshot_date_start = PREP_CONFIG["snapshot"]["start_date"]
        _snapshot_date_end = PREP_CONFIG["snapshot"]["end_date"]

        snapshot_date = (
            pd.date_range(
                start=_snapshot_date_start,
                end=_snapshot_date_end,
                freq="W-MON",
            )
            .strftime("%Y-%m-%d")
            .tolist()[-1]
        )

        df_snapshot_cust = self.customers.get_for_date(snapshot_date=snapshot_date)
        logger.info(
            "Getting snapshot for: "
            + snapshot_date
            + ", total customers: "
            + str(df_snapshot_cust.shape[0]),
        )

        df_snapshot_cust.reset_index(inplace=True)
        customer = df_snapshot_cust[df_snapshot_cust["agreement_id"] == customer_id]

        if not customer.empty:
            customer_snapshot = self.get_snapshot_features_for_date(
                snapshot_date=snapshot_date,
                customer_id=customer_id,
            )
            return customer_snapshot
        else:
            logger.info(
                "Customer not found on the dataset. Customer probaby \
                is on the onboard phase / left the service a long time ago. In other words, it churned",
            )
            return customer

    def _load_data(
        self,
        company_id: str,
        model_training: bool,
        snapshot_config: dict,
        input_files: dict,
    ) -> list:
        return [
            Customers(
                company_id=company_id,
                model_training=model_training,
                onboarding_weeks=snapshot_config.get("onboarding_weeks", 12),
                input_file=input_files.get("customers"),
                db=self.adb,
            ),
            Events(
                company_id=company_id,
                model_training=model_training,
                forecast_weeks=snapshot_config.get("forecast_weeks", 4),
                events_last_n_weeks=snapshot_config.get("events_last_n_weeks", 4),
                average_last_n_weeks=snapshot_config.get("average_last_n_weeks", 10),
                input_file=input_files.get("events"),
                db=self.postgres_db,
            ),
            Orders(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("orders"),
                db=self.adb,
            ),
            CRMSegments(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("crm_segments"),
                db=self.adb,
            ),
            Complaints(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("complaints"),
                complaints_last_n_weeks=snapshot_config.get("complaints_last_n_weeks"),
                db=self.adb,
            ),
        ]

    def _load_datasets(self) -> None:
        logger.info("Loading datasets...")
        for dataset in self.data_set:
            dataset.load()

    def _get_features(self, snapshot_date: datetime) -> pd.DataFrame:
        logger.info("Generating features from date " + str(snapshot_date))
        features = []
        for cls in self.datasets:
            logger.info(f"Generating features for {cls.__class__.__name__}...")
            f = cls.get_features_for_snapshot(snapshot_date)
            features.append(f)

        logger.info("Merging features...")
        df = reduce(
            lambda left, right: pd.merge(left, right, on="agreement_id", how="outer"),
            features,
        )

        # date features
        df["year"] = snapshot_date.year
        df["month"] = snapshot_date.month
        df["week"] = snapshot_date.week

        df.fillna(self._get_default_values(), inplace=True)

        return df

    def _get_default_values(self) -> dict:
        default_values = {}
        for ds in self.datasets:
            default_values.update(ds.get_default_values())
        return default_values

    def _add_labels(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        logger.info("Processing labels...")
        if self.model_training:
            # Set forecast status
            df["forecast_status"] = np.where(
                df["number_of_forecast_orders"] == 0,
                LABEL_TEXT_CHURNED,
                df["forecast_status"],
            )
            # Setting status 'churned'
            df["snapshot_status"] = np.where(
                (df["weeks_since_last_delivery"] > self.buy_history_churn_weeks)
                & (df["number_of_forecast_orders"] == 0)
                & (df["number_of_total_orders"] == 0),
                LABEL_TEXT_CHURNED,
                df["snapshot_status"],
            )
            df.drop(["number_of_forecast_orders"], inplace=True, axis=1)
        else:  # When model_training is False
            if (
                df.iloc[0]["weeks_since_last_delivery"] > self.buy_history_churn_weeks
            ) & (df.iloc[0]["number_of_total_orders"] == 0):
                df["snapshot_status"] = LABEL_TEXT_CHURNED
            df.drop(["forecast_status"], inplace=True, axis=1)

        # Handle delete customers with zero events and order history (set status to deleted)
        df["snapshot_status"] = np.where(
            (df["weeks_since_last_delivery"] == -1)
            & (df["number_of_total_orders"] == 0)
            & (df["agreement_status"] == LABEL_TEXT_DELETED),
            LABEL_TEXT_DELETED,
            df["snapshot_status"],
        )

        return df
