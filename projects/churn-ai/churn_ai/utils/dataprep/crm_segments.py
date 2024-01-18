import re
import numpy as np
import pandas as pd
import logging
from . import datasource as source


class CRM_segments(source.Datasource):
    df = pd.DataFrame()
    return_columns = ["agreement_id", "planned_delivery"]

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        Loads crm segments dataset
        :param filepath:
        :return:
        """
        mult = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")

        if self.filepath is None:
            return pd.DataFrame()

        self.df = pd.read_csv(self.filepath, low_memory=False)
        self.df = self.df.replace(np.nan, "", regex=True)

        self.df["delivery_date"] = pd.to_datetime(
            self.df.current_delivery_year * 1000
            + self.df.current_delivery_week * 10
            + 0,
            format="%Y%W%w",
        )

        print("Total crm segments: " + str(self.df.shape[0]))
        print("CRM table column names:" + str(self.df.columns))

    def get_for_date(self, snapshot_date, model_training=False):
        """
        Returns crm_segments for all customers for given date
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df

        if model_training == True:
            return self.df.loc[self.df.delivery_date <= snapshot_date].copy()
        else:
            return self.df.loc[self.df.delivery_date >= snapshot_date].copy()

    @staticmethod
    def get_customer_segment(df, agreement_id):
        """
        Returns latest crm segment for the customer
        :param df:
        :param agreement_id:
        :return: dataframe
        """
        if df.empty:
            return df
        df_crm_cust = df[df.agreement_id == agreement_id]
        df_crm_cust = df_crm_cust[
            df_crm_cust.delivery_date == df_crm_cust.delivery_date.max()
        ]

        return df_crm_cust[CRM_segments.return_columns]
