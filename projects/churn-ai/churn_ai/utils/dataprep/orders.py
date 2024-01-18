import pandas as pd
import logging
import math
import numpy as np
from . import datasource as source


class Orders(source.Datasource):
    df = pd.DataFrame()

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        :param source_file:
        :return: orders Dataframe
        """
        if self.filepath is None:
            return

        self.df = pd.read_csv(self.filepath, low_memory=False)
        self.df["delivery_date"] = pd.to_datetime(self.df.delivery_date)
        print("Total orders: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date):
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    def get_customer_orders(
        self, df_orders, agreement_id, snapshot_date, model_training=False
    ):
        """
        :param df_orders: orders df
        :param agreement_id: agreement / customer id
        :param snapshot_date: date of the data snapshot
        :param model_training: Flag to indicate if forecast features should be generated
        :return: weeks_since_last_delivery = returns -1 if customer had no orders
        """
        if self.df.empty:
            return self.df
        dict_out = {}

        df = df_orders[df_orders.agreement_id == agreement_id]

        dict_out["agreement_id"] = agreement_id
        dict_out["snapshot_date"] = snapshot_date

        # snapshot-date
        dict_out["year"] = pd.to_datetime(snapshot_date).year
        dict_out["month"] = pd.to_datetime(snapshot_date).month
        dict_out["week"] = pd.to_datetime(snapshot_date).week

        # deliver dates
        dict_out["number_of_total_orders"] = df.loc[
            df.delivery_date <= snapshot_date
        ].shape[0]
        if model_training:
            dict_out["number_of_forecast_orders"] = df.loc[
                df.delivery_date > snapshot_date
            ].shape[0]

        # weeks_since_last_delivery
        weeks_since_last_delivery = -1
        snapshot_df = df.loc[df.delivery_date <= snapshot_date]
        if not snapshot_df.empty:
            max_delivery_date = snapshot_df.delivery_date.max()
            date_diff = pd.to_datetime(snapshot_date) - max_delivery_date
            weeks_since_last_delivery = math.trunc(date_diff / np.timedelta64(1, "W"))
        dict_out["weeks_since_last_delivery"] = weeks_since_last_delivery

        return pd.DataFrame([dict_out])
