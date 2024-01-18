import logging
import numpy as np
import pandas as pd
from . import datasource as source


class Complaints(source.Datasource):
    df = pd.DataFrame()
    return_columns = [
        "agreement_id",
        "category",
        "weeks_since_last_complaint",
        "total_complaints",
        "number_of_complaints_last_N_weeks",
    ]

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        Loads complaints dataset
        :param filepath:
        :return:
        """
        if self.filepath is None:
            return

        self.df = pd.read_csv(self.filepath, low_memory=False)
        self.df.registration_date = pd.to_datetime(self.df.registration_date)
        self.df.category = self.df.category.str.lower().replace(" ", "")
        self.df["delivery_date"] = pd.to_datetime(
            self.df.delivery_year * 1000 + self.df.delivery_week * 10 + 0,
            format="%Y%W%w",
        )
        print("Total complaints: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date):
        """
        Returns crm_segments for all customers for given date
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.delivery_date <= snapshot_date]

    @staticmethod
    def get_customer_complaints(
        df, agreement_id, snapshot_date, complaints_last_n_weeks=4
    ):
        """
        Returns latest crm segment for the customer
        :param df:
        :param agreement_id:
        :param snapshot_date:
        :param complaints_last_n_weeks: number of complaints last N weeks
        :return: dataframe
        """
        if df.empty:
            return df
        df_cust_complaints = df[df.agreement_id == agreement_id]
        df_cust_complaints_last = df_cust_complaints[
            df_cust_complaints.delivery_date == df_cust_complaints.delivery_date.max()
        ]
        if df_cust_complaints.empty:
            weeks_since_last_complaint = -1
            total_complaints = -1
            last_n_complaints = -1
            category = ""
        else:
            total_complaints = df_cust_complaints.shape[0]
            df_cust_complaints_last = df_cust_complaints_last.iloc[:1]
            weeks_since_last_complaint = pd.to_datetime(snapshot_date) - pd.to_datetime(
                df_cust_complaints_last.delivery_date
            )
            weeks_since_last_complaint = int(
                weeks_since_last_complaint / np.timedelta64(1, "W")
            )
            dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
                weeks=-complaints_last_n_weeks
            )
            last_n_complaints = df_cust_complaints[
                df_cust_complaints.delivery_date >= dt_from
            ].shape[0]
            category = df_cust_complaints_last.iloc[0]["category"]

        customer_complaint_snapshot = {
            "agreement_id": agreement_id,
            "total_complaints": total_complaints,
            "category": category,
            "weeks_since_last_complaint": weeks_since_last_complaint,
            "number_of_complaints_last_N_weeks": last_n_complaints,
        }

        return pd.DataFrame(customer_complaint_snapshot, index=[0])
