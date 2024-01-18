import re
import logging
import pandas as pd
from . import datasource as source


class Bisnode(source.Datasource):
    df = pd.DataFrame()

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        Loads bisnode dataset
        :param filepath:
        :return:
        """
        bisnode_columns = [
            "agreement_id",
            "impulsiveness",
            "created_at",
            "cultural_class",
            "perceived_purchasing_power",
            "consumption",
            "children_probability",
            "financial_class",
            "life_stage",
            "type_of_housing",
            "confidence_level",
        ]

        if self.filepath is None:
            return

        self.df = pd.read_csv(self.filepath, low_memory=False)
        print("Bisnode dataset total records:" + str(self.df.shape[0]))
        # Selecting in columns of interest
        self.df["children_probability"] = self.df[
            ["probability_children_0_to_6", "probability_children_7_to_17"]
        ].max(axis=1)
        self.df = self.df[bisnode_columns].dropna()
        print(
            "Bisnode dataset after removing missing value/records:"
            + str(self.df.shape[0])
        )
        # Cleaning label features
        mult = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")

        self.df.created_at = pd.to_datetime(self.df.created_at)
        self.df.cultural_class = (
            self.df.cultural_class.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        self.df.perceived_purchasing_power = (
            self.df.perceived_purchasing_power.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        self.df.financial_class = (
            self.df.financial_class.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        self.df.life_stage = (
            self.df.life_stage.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        self.df.type_of_housing = (
            self.df.type_of_housing.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        self.df.confidence_level = (
            self.df.confidence_level.apply(lambda x: mult.sub(r"\1", x))
            .str.replace(" ", "", regex=True)
            .str.replace(".", "_", regex=True)
            .str.lower()
        )
        print("Total bisnodes: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date):
        """
        Returns rows for all customers for given date
        :param snapshot_date:
        :return: cleaned bisnode DataFrame
        """
        if self.df.empty:
            return self.df
        diff = self.df.created_at - pd.to_datetime(snapshot_date)
        df_min = diff.abs().groupby(self.df["agreement_id"]).idxmin()
        return self.df.loc[df_min]

    @staticmethod
    def get_for_customer(df, agreement_id, snapshot_date, complaints_last_n_weeks=4):
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
        df_bis = df[df.agreement_id == agreement_id]

        return df_bis.drop(columns=["created_at"])
