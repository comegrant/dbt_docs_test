import pandas as pd
import logging
from . import datasource as source


class Customers(source.Datasource):

    df = pd.DataFrame()
    columns_out = [
        "agreement_id",
        "agreement_start_year",
        "agreement_start_week",
        "agreement_first_delivery_year",
        "agreement_first_delivery_week",
        "agreement_status",
        "agreement_start_date",
    ]

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        Loads customers dataset
        :param filepath:
        :return:
        """
        if self.filepath is None:
            return

        df = pd.read_csv(self.filepath, low_memory=False)
        # Delete all customers with status 'deleted' (we don't have any records in events dataset for them)
        customers_to_delete = df[df.agreement_status == "deleted"].shape[0]
        print(
            "Total customers with delete status (will be ignored): "
            + str(customers_to_delete)
        )
        df = df.loc[df.agreement_status != "deleted"].copy()

        df["agreement_creation_date"] = pd.to_datetime(df.agreement_creation_date)
        df["agreement_start_date"] = pd.to_datetime(df.agreement_start_date)
        df["agreement_first_delivery_date"] = pd.to_datetime(
            df.agreement_first_delivery_date
        )
        df["last_delivery_date"] = pd.to_datetime(df.last_delivery_date)
        df["next_estimated_delivery_date"] = pd.to_datetime(
            df.next_estimated_delivery_date
        )
        df["agreement_first_delivery_year"] = df.agreement_first_delivery_year.fillna(
            0
        ).astype(int)
        df["agreement_first_delivery_week"] = df.agreement_first_delivery_week.fillna(
            0
        ).astype(int)
        self.df = df
        print("Total Customers: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date, onboarding_weeks=12):
        """
        Returns all customers older than snapshot_date + onboarding_weeks
        :param snapshot_date: Date from where the snapshot should be made (YYYY-MM-DD)
        :param onboarding_weeks:
        :return: cleaned customers DataFrame
        """
        if self.df.empty:
            return self.df
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(weeks=-onboarding_weeks)
        df = self.df.loc[self.df.agreement_start_date <= dt_from]
        return df[self.columns_out]
