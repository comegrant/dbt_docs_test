# Holds the dataprep logic.

import math
import time
import numpy as np
import pandas as pd
from tqdm import tqdm

from utils.dataprep.customers import Customers
from utils.dataprep.events import Events
from utils.dataprep.orders import Orders
from utils.dataprep.crm_segments import CRM_segments
from utils.dataprep.complaints import Complaints
from utils.dataprep.bisnode import Bisnode
from utils.dataprep import common


class Gen:
    """
    INFO: Main method for generating datasets for training, evaluation and estimation
    """

    def __init__(self, config, out_dir="", save_snapshot=None):
        """
        :param config: config_input_files i.e. just the path to the source files but not the content
        """
        self.config = config
        self.customers = Customers(self.config["input_files"]["customers"])
        self.events = Events(self.config["input_files"]["events"])
        self.orders = Orders(self.config["input_files"]["orders"])
        self.crm_segments = CRM_segments(self.config["input_files"]["crm_segments"])
        self.complaints = Complaints(self.config["input_files"]["complaints"])
        self.bisnode = Bisnode(self.config["input_files"]["bisnode"])
        self.save_snapshot = save_snapshot

    def get_customer_snapshot(
        self,
        customer,
        snapshot_date,
        df_orders,
        df_crm_segments,
        df_complaints,
        df_events,
        df_bisnode,
        forecast_weeks=4,
        buy_history_churn_weeks=4,
        model_training=False,
    ):
        """
        :param customer: Customer DataFrame (single customer)
        :param snapshot_date: snapshot data
        :param df_events: Events DataFrame
        :param df_orders: Orders DataFrame
        :param forecast_weeks: Number of forecast weeks
        :param buy_history_churn_weeks: Set customer status to churned if customer didn't make any orders within this period
        :param model_training: Prepare dataset with future features (used for model training)
        :return: None or Merged snapshot from events, orders and customer dataset for given snapshot date
        """
        _churned_status = common.label_text_churned
        agreement_id = customer.agreement_id

        # orders
        snapshot = self.orders.get_customer_orders(
            df_orders, agreement_id, snapshot_date, model_training=model_training
        )

        # Return None if weeks_since_last_deliver > forecast_weeks
        if snapshot.at[0, "weeks_since_last_delivery"] > forecast_weeks:
            return None

        # customer
        customer_age_diff = (
            pd.to_datetime(snapshot_date) - customer.agreement_start_date
        )
        snapshot["customer_since_weeks"] = math.trunc(
            customer_age_diff / np.timedelta64(1, "W")
        )

        # events
        cust_events = self.events.get_customer_events(
            df_events=df_events,
            agreement_id=agreement_id,
            forecast_weeks=forecast_weeks,
            snapshot_date=snapshot_date,
        )

        # crm_segments
        cust_crm_segments = self.crm_segments.get_customer_segment(
            df_crm_segments, agreement_id=agreement_id
        )

        # Complaints
        cust_complaints = self.complaints.get_customer_complaints(
            df_complaints, snapshot_date=snapshot_date, agreement_id=agreement_id
        )

        # Bisnode
        bisnode_data = self.bisnode.get_for_customer(
            df_bisnode, agreement_id=agreement_id, snapshot_date=snapshot_date
        )

        if not bisnode_data.empty:
            snapshot = snapshot.merge(bisnode_data, on="agreement_id", how="left")
        if not cust_complaints.empty:
            snapshot = snapshot.merge(cust_complaints, on="agreement_id", how="left")
        if not cust_crm_segments.empty:
            snapshot = snapshot.merge(cust_crm_segments, on="agreement_id", how="left")
        if not cust_events.empty:
            snapshot = snapshot.merge(cust_events, on="agreement_id", how="left")

        if model_training:
            # Setting status 'churned'
            if snapshot.iloc[0]["number_of_forecast_orders"] == 0:
                snapshot["forecast_status"] = _churned_status
            if (
                snapshot.iloc[0]["weeks_since_last_delivery"] > buy_history_churn_weeks
                and snapshot.iloc[0]["number_of_forecast_orders"] == 0
                and snapshot.iloc[0]["number_of_total_orders"] == 0
            ):
                snapshot["snapshot_status"] = _churned_status
            snapshot.drop(["number_of_forecast_orders"], inplace=True, axis=1)
        else:  # When model_training is False
            if (
                snapshot.iloc[0]["weeks_since_last_delivery"] > buy_history_churn_weeks
                and snapshot.iloc[0]["number_of_total_orders"] == 0
            ):
                snapshot["snapshot_status"] = _churned_status
            snapshot.drop(["forecast_status"], inplace=True, axis=1)

        # Handle delete customers with zero events and order history (set status to deleted)
        if (
            snapshot.iloc[0]["weeks_since_last_delivery"] == -1
            and snapshot.iloc[0]["number_of_total_orders"] == 0
            and customer.agreement_status == common.label_text_deleted
        ):
            snapshot["snapshot_status"] = common.label_text_deleted

        return snapshot

    def main(self, model_training=False):
        """
        Generates dataset for model
        :return:
        """
        print("Starting with prep..")

        _snapshot_date_start = self.config["snapshot"]["start_date"]
        _snapshot_date_end = self.config["snapshot"]["end_date"]
        _forecast_weeks = self.config["snapshot"]["forecast_weeks"]
        _buy_history_churn_weeks = self.config["snapshot"]["buy_history_churn_weeks"]
        _output_dir = self.config["output_dir"]
        _output_file_prefix = "snapshot_"
        _company_id = self.config["company_id"]

        # Get snapshot dates based on company orders cutoff date
        if _company_id == "5E65A955-7B1A-446C-B24F-CFE576BF52D7":
            snapshot_dates = (
                pd.date_range(
                    start=_snapshot_date_start,
                    end=_snapshot_date_end,
                    freq="W-FRI",  # RN orders cutoff Thursday
                )
                .strftime("%Y-%m-%d")
                .tolist()
            )
        else:
            snapshot_dates = (
                pd.date_range(
                    start=_snapshot_date_start,
                    end=_snapshot_date_end,
                    freq="W-WED",  # All others orders cutoff Tuesday
                )
                .strftime("%Y-%m-%d")
                .tolist()
            )

        assert len(snapshot_dates) >= 1

        # Loading datasets
        self.bisnode.load()
        self.customers.load()
        self.events.load()
        self.orders.load()
        self.crm_segments.load()
        self.complaints.load()

        snap_list = []

        for snapshot_date in snapshot_dates:
            snapshot_dt_start = time.time()

            # Get customers for given snapshot date
            df_snapshot_cust = self.customers.get_for_date(snapshot_date=snapshot_date)
            print(
                "Getting snapshot for: "
                + snapshot_date
                + ", total customers: "
                + str(df_snapshot_cust.shape[0])
            )

            out_file = (
                _output_dir
                + _output_file_prefix
                + snapshot_date.replace("-", "")
                + ".csv"
            )
            snap_list.append(out_file)
            df_out_list = []
            dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
                weeks=_forecast_weeks
            )
            df_snapshot_events = self.events.get_for_date(dt_from)
            df_snapshot_orders = self.orders.get_for_date(dt_from)
            df_snapshot_crm_segments = self.crm_segments.get_for_date(
                snapshot_date, model_training=model_training
            )
            df_snapshot_complaints = self.complaints.get_for_date(snapshot_date)
            df_snapshot_bisnode = self.bisnode.get_for_date(snapshot_date)

            print("Output file: " + out_file)

            # Iterate through every customer
            df_snapshot_cust.reset_index(inplace=True)

            for _, customer in tqdm(
                df_snapshot_cust.iterrows(), total=df_snapshot_cust.shape[0]
            ):
                customer_snapshot = self.get_customer_snapshot(
                    customer,
                    snapshot_date=snapshot_date,
                    df_events=df_snapshot_events,
                    df_orders=df_snapshot_orders,
                    df_crm_segments=df_snapshot_crm_segments,
                    df_complaints=df_snapshot_complaints,
                    df_bisnode=df_snapshot_bisnode,
                    forecast_weeks=_forecast_weeks,
                    buy_history_churn_weeks=_buy_history_churn_weeks,
                    model_training=model_training,
                )

                if customer_snapshot is not None:
                    df_out_list.append(customer_snapshot)

            with open(out_file, "a") as f:
                df_out = pd.concat(df_out_list, ignore_index=True)
                df_out.to_csv(out_file, mode="a", header=f.tell() == 0, index=False)
                del df_out

            del df_out_list

            snapshot_dt_end = time.time()
            print(
                "Snapshot done in "
                + str(int(snapshot_dt_end - snapshot_dt_start))
                + " (sec)"
            )

            if self.save_snapshot is not None:
                self.save_snapshot(out_file)

        return snap_list

    def main_single_customer(self, customer_id):
        _snapshot_date_start = self.config["snapshot"]["start_date"]
        _snapshot_date_end = self.config["snapshot"]["end_date"]
        _forecast_weeks = self.config["snapshot"]["forecast_weeks"]
        _buy_history_churn_weeks = self.config["snapshot"]["buy_history_churn_weeks"]

        snapshot_date = (
            pd.date_range(
                start=_snapshot_date_start, end=_snapshot_date_end, freq="W-MON"
            )
            .strftime("%Y-%m-%d")
            .tolist()[-1]
        )

        # Loading datasets
        self.bisnode.load()
        self.customers.load()
        self.events.load()
        self.orders.load()
        self.crm_segments.load()
        self.complaints.load()

        df_snapshot_cust = self.customers.get_for_date(snapshot_date=snapshot_date)
        print(
            "Getting snapshot for: "
            + snapshot_date
            + ", total customers: "
            + str(df_snapshot_cust.shape[0])
        )

        df_snapshot_cust.reset_index(inplace=True)
        print(df_snapshot_cust.head())
        print(customer_id)
        customer = df_snapshot_cust[df_snapshot_cust["agreement_id"] == customer_id]

        if not customer.empty:
            dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
                weeks=_forecast_weeks
            )
            df_snapshot_events = self.events.get_for_date(dt_from)
            df_snapshot_orders = self.orders.get_for_date(dt_from)
            df_snapshot_crm_segments = self.crm_segments.get_for_date(snapshot_date)
            df_snapshot_complaints = self.complaints.get_for_date(snapshot_date)
            df_snapshot_bisnode = self.bisnode.get_for_date(snapshot_date)

            customer_snapshot = self.get_customer_snapshot(
                customer,
                snapshot_date=snapshot_date,
                df_events=df_snapshot_events,
                df_orders=df_snapshot_orders,
                df_crm_segments=df_snapshot_crm_segments,
                df_complaints=df_snapshot_complaints,
                df_bisnode=df_snapshot_bisnode,
                forecast_weeks=_forecast_weeks,
                buy_history_churn_weeks=_buy_history_churn_weeks,
                model_training=False,
            )
            return customer_snapshot
        else:
            print(
                "Customer not found on the dataset. Customer probaby \
                is on the onboard phase / left the service a long time ago. In other words, it churned"
            )
            return customer
