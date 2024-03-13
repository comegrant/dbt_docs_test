# Holds the dataprep logic.

import logging
import time
from datetime import datetime
from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from customer_churn.config import PREP_CONFIG
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
        company_id: str,
        out_dir: str = "",
        save_snapshot: bool = False,
        model_training: bool = False,
        prep_config: dict = PREP_CONFIG,
    ) -> None:
        """
        :param config: config_input_files i.e. just the path to the source files but not the content
        """
        self.feature_set = self._get_feature_set(
            company_id,
            model_training,
            prep_config["snapshot"],
            prep_config["input_files"],
        )
        self.save_snapshot = save_snapshot
        self.out_dir = out_dir
        self.buy_history_churn_weeks = prep_config["snapshot"].get(
            "buy_history_churn_weeks",
            4,
        )
        self.model_training = model_training

    def _get_feature_set(
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
            ),
            Events(
                company_id=company_id,
                model_training=model_training,
                forecast_weeks=snapshot_config.get("forecast_weeks", 4),
                events_last_n_weeks=snapshot_config.get("events_last_n_weeks", 4),
                average_last_n_weeks=snapshot_config.get("average_last_n_weeks", 10),
                input_file=input_files.get("events"),
            ),
            Orders(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("orders"),
            ),
            CRMSegments(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("crm_segments"),
            ),
            Complaints(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("complaints"),
                complaints_last_n_weeks=snapshot_config.get("complaints_last_n_weeks"),
            ),
            Bisnode(
                company_id=company_id,
                model_training=model_training,
                input_file=input_files.get("bisnode"),
            ),
        ]

    def get_snapshot_features_for_date(
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

    def generate_features(self) -> list:
        logger.info("Starting with preperation of features...")

        _forecast_weeks = PREP_CONFIG["snapshot"]["forecast_weeks"]
        _buy_history_churn_weeks = PREP_CONFIG["snapshot"]["buy_history_churn_weeks"]
        _output_dir = PREP_CONFIG["output_dir"]
        _output_file_prefix = "snapshot_"

        snapshot_dates = self._get_snapshot_dates_based_on_company(
            PREP_CONFIG["snapshot"]["start_date"],
            PREP_CONFIG["snapshot"]["end_date"],
            PREP_CONFIG["company_id"],
        )

        assert len(snapshot_dates) >= 1

        snap_list = []

        for snapshot_date in tqdm(snapshot_dates):
            snapshot_dt_start = time.time()

            # Get customers for given snapshot date
            logger.info(
                f"Getting snapshot for: {snapshot_date}",
            )

            out_file = (
                _output_dir
                + _output_file_prefix
                + snapshot_date.replace("-", "")
                + ".csv"
            )
            snap_list.append(out_file)

            logger.info(f"Output file: {out_file}")

            # Create snapshot
            df_out = self.get_snapshot_features_for_date(
                snapshot_date=snapshot_date,
                forecast_weeks=_forecast_weeks,
                buy_history_churn_weeks=_buy_history_churn_weeks,
            )

            with Path.open(out_file, "a") as f:
                df_out.to_csv(out_file, mode="a", header=f.tell() == 0, index=False)

            snapshot_dt_end = time.time()
            logger.info(
                "Snapshot done in "
                + str(int(snapshot_dt_end - snapshot_dt_start))
                + " (sec)",
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

    def _get_features(self, snapshot_date: datetime) -> pd.DataFrame:
        logger.info("Generating features from date " + str(snapshot_date))
        features = []
        for cls in self.feature_set:
            logger.info(f"Generating features for {cls.__class__.__name__}...")
            f = cls.get_features_for_snapshot(snapshot_date)
            if f.empty:
                continue
            features.append(f)

        logger.info("Merging features...")
        df = reduce(
            lambda left, right: pd.merge(left, right, on="agreement_id"),
            features,
        )

        logging.info(df)

        return df

    def _add_labels(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        # TODO: Refactor to be all customers in groupby instead of pr customer
        logger.info("Processing labels...")
        if self.model_training:
            df["forecast_status"] = np.where(
                df["number_of_forecast_orders"] == 0,
                LABEL_TEXT_CHURNED,
            )
            # Setting status 'churned'
            if df.iloc[0]["number_of_forecast_orders"] == 0:
                df["forecast_status"] = LABEL_TEXT_CHURNED
            if (
                df.iloc[0]["weeks_since_last_delivery"] > self.buy_history_churn_weeks
                and df.iloc[0]["number_of_forecast_orders"] == 0
                and df.iloc[0]["number_of_total_orders"] == 0
            ):
                df["snapshot_status"] = LABEL_TEXT_CHURNED
            df.drop(["number_of_forecast_orders"], inplace=True, axis=1)
        else:  # When model_training is False
            if (
                df.iloc[0]["weeks_since_last_delivery"] > self.buy_history_churn_weeks
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
