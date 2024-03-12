# Holds the dataprep logic.

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from customer_churn.constants import LABEL_TEXT_CHURNED, LABEL_TEXT_DELETED, CompanyID
from customer_churn.dataset.bisnode import Bisnode
from customer_churn.dataset.complaints import Complaints
from customer_churn.dataset.crm_segments import CRMSegments
from customer_churn.dataset.customers import Customers
from customer_churn.dataset.events import Events
from customer_churn.dataset.orders import Orders

logger = logging.getLogger(__name__)


class Features:
    """
    INFO: Main method for generating datasets for training, evaluation and estimation
    """

    def __init__(
        self,
        config: str,
        out_dir: str = "",
        save_snapshot: bool | None = None,
    ) -> None:
        """
        :param config: config_input_files i.e. just the path to the source files but not the content
        """
        self.config = config
        self.feature_set = [
            Customers(self.config["input_files"]["customers"]),
            Events(self.config["input_files"]["events"]),
            Orders(self.config["input_files"]["orders"]),
            CRMSegments(self.config["input_files"]["crm_segments"]),
            Complaints(self.config["input_files"]["complaints"]),
            Bisnode(self.config["input_files"]["bisnode"]),
        ]
        self.save_snapshot = save_snapshot
        self.out_dir = out_dir

    def get_snapshot_features_for_date(
        self,
        snapshot_date: str,
        forecast_weeks: int = 4,
        buy_history_churn_weeks: int = 4,
        model_training: bool = False,
        customer_id: str | None = None,
    ) -> None | pd.DataFrame:
        """
        :param customer: Customer DataFrame (single customer)
        :param snapshot_date: snapshot data
        :param df_events: Events DataFrame
        :param df_orders: Orders DataFrame
        :param forecast_weeks: Number of forecast weeks
        :param buy_history_churn_weeks:
            Set customer status to churned if customer didn't make any orders within this period
        :param model_training: Prepare dataset with future features (used for model training)
        :return: None or Merged snapshot from events, orders and customer dataset for given snapshot date
        """

        if customer_id:
            raise NotImplementedError("Features for single customer not implemented yet")

        snapshot_features = self._get_features(
            snapshot_date=snapshot_date,
            forecast_weeks=forecast_weeks,
            model_training=model_training,
        )

        snapshot_features = snapshot_features.apply(
            lambda agreement: self._get_label_for_agreement(agreement, buy_history_churn_weeks, model_training),
        )

        return snapshot_features

    def generate_features(self, model_training: bool = False) -> list:
        logger.info("Starting with preperation of features...")

        _forecast_weeks = self.config["snapshot"]["forecast_weeks"]
        _buy_history_churn_weeks = self.config["snapshot"]["buy_history_churn_weeks"]
        _output_dir = self.config["output_dir"]
        _output_file_prefix = "snapshot_"

        snapshot_dates = self._get_snapshot_dates_based_on_company(
            self.config["snapshot"]["start_date"],
            self.config["snapshot"]["end_date"],
            self.config["company_id"],
        )

        assert len(snapshot_dates) >= 1

        # Load datasets
        self._load_datasets()

        snap_list = []

        for snapshot_date in tqdm(snapshot_dates):
            snapshot_dt_start = time.time()

            # Get customers for given snapshot date
            logger.info(
                f"Getting snapshot for: {snapshot_date}",
            )

            out_file = _output_dir + _output_file_prefix + snapshot_date.replace("-", "") + ".csv"
            snap_list.append(out_file)

            logger.info(f"Output file: {out_file}")

            # Create snapshot
            df_out = self.get_snapshot_features_for_date(
                snapshot_date=snapshot_date,
                forecast_weeks=_forecast_weeks,
                buy_history_churn_weeks=_buy_history_churn_weeks,
                model_training=model_training,
            )

            with Path.open(out_file, "a") as f:
                df_out.to_csv(out_file, mode="a", header=f.tell() == 0, index=False)

            snapshot_dt_end = time.time()
            logger.info(
                "Snapshot done in " + str(int(snapshot_dt_end - snapshot_dt_start)) + " (sec)",
            )

            if self.save_snapshot is not None:
                self.save_snapshot(out_file)

        return snap_list

    def _get_snapshot_dates_based_on_company(
        self,
        snapshot_start_date: str,
        snapshot_end_date: str,
        company_id: str,
    ) -> list:
        # Get snapshot dates based on company orders cutoff date
        if company_id == CompanyID.RN:
            return (
                pd.date_range(
                    start=snapshot_start_date,
                    end=snapshot_end_date,
                    freq="W-FRI",  # RN orders cutoff Thursday
                )
                .strftime("%Y-%m-%d")
                .tolist()
            )
        else:
            return (
                pd.date_range(
                    start=snapshot_start_date,
                    end=snapshot_end_date,
                    freq="W-WED",  # All others orders cutoff Tuesday
                )
                .strftime("%Y-%m-%d")
                .tolist()
            )

    def _load_datasets(self) -> None:
        logger.info("Loading datasets...")
        for dataset in self.feature_set:
            dataset.load()

    def _get_features(self, snapshot_date: datetime, forecast_weeks: int) -> pd.DataFrame:
        logger.info("Generating features for snapshot...")
        features = []
        for cls in self.feature_set:
            features.append(cls.get_features_for_snapshot(snapshot_date, forecast_weeks))

        df = pd.concat(features)

        return df

    def _get_label_for_agreement(
        self,
        df: pd.DataFrame,
        buy_history_churn_weeks: int,
        model_training: bool,
    ) -> pd.DataFrame:
        logger.info("Processing labels...")
        if model_training:
            # Setting status 'churned'
            if df.iloc[0]["number_of_forecast_orders"] == 0:
                df["forecast_status"] = LABEL_TEXT_CHURNED
            if (
                df.iloc[0]["weeks_since_last_delivery"] > buy_history_churn_weeks
                and df.iloc[0]["number_of_forecast_orders"] == 0
                and df.iloc[0]["number_of_total_orders"] == 0
            ):
                df["snapshot_status"] = LABEL_TEXT_CHURNED
            df.drop(["number_of_forecast_orders"], inplace=True, axis=1)
        else:  # When model_training is False
            if (
                df.iloc[0]["weeks_since_last_delivery"] > buy_history_churn_weeks
                and df.iloc[0]["number_of_total_orders"] == 0
            ):
                df["snapshot_status"] = LABEL_TEXT_CHURNED
            df.drop(["forecast_status"], inplace=True, axis=1)

        # Handle delete customers with zero events and order history (set status to deleted)
        if (
            df.iloc[0]["weeks_since_last_delivery"] == -1
            and df.iloc[0]["number_of_total_orders"] == 0
            and df["agreement_status"] == LABEL_TEXT_DELETED
        ):
            df["snapshot_status"] = LABEL_TEXT_DELETED

        return df

    def generate_features_for_customer(self, customer_id: int) -> pd.DataFrame:
        logger.info(f"Generating features for single customer {customer_id}")

        _snapshot_date_start = self.config["snapshot"]["start_date"]
        _snapshot_date_end = self.config["snapshot"]["end_date"]
        _forecast_weeks = self.config["snapshot"]["forecast_weeks"]
        _buy_history_churn_weeks = self.config["snapshot"]["buy_history_churn_weeks"]

        snapshot_date = (
            pd.date_range(
                start=_snapshot_date_start,
                end=_snapshot_date_end,
                freq="W-MON",
            )
            .strftime("%Y-%m-%d")
            .tolist()[-1]
        )

        self._load_datasets()

        df_snapshot_cust = self.customers.get_for_date(snapshot_date=snapshot_date)
        logger.info(
            "Getting snapshot for: " + snapshot_date + ", total customers: " + str(df_snapshot_cust.shape[0]),
        )

        df_snapshot_cust.reset_index(inplace=True)
        customer = df_snapshot_cust[df_snapshot_cust["agreement_id"] == customer_id]

        if not customer.empty:
            customer_snapshot = self.get_snapshot_features_for_date(
                snapshot_date=snapshot_date,
                forecast_weeks=_forecast_weeks,
                buy_history_churn_weeks=_buy_history_churn_weeks,
                model_training=False,
                customer_id=customer_id,
            )
            return customer_snapshot
        else:
            logger.info(
                "Customer not found on the dataset. Customer probaby \
                is on the onboard phase / left the service a long time ago. In other words, it churned",
            )
            return customer
