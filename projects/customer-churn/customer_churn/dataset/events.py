import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from lmkgroup_ds_utils.constants import Company
from lmkgroup_ds_utils.db.connector import DB

from customer_churn.paths import SQL_DIR

logger = logging.getLogger(__name__)


DEFAULT_START_STATUS = "active"

STATUS_CHANGE = ["change_status__freezed", "change_status__activated"]

NORMAL_ACTIVITIES = [
    "week_previewed",
    "week_breakfast_previewed",
    "week_planned",
    "valgfri_plan_next_week",
    "change_menu",
    "rated_recipe",
    "_product_added",
    "_product_viewed",
]

ACCOUNT_MANAGEMENT = [
    "change_payment",
    "_postcodefound",
    "update_personalia",
    "passwordformselected",
    "_emailformcompleted",
    "telephoneformselected",
    "_telephoneauthfailure",
    "_postcodenotfound",
    "change_address",
    "_postcodenotfound",
]


LABEL_TEXT_ACTIVE = "active"
LABEL_TEXT_DELETED = "deleted"
LABEL_TEXT_CHURNED = "churned"

SCHEMAS = {
    Company.GL: "js",
    Company.LMK: "javascript_lmk",
    Company.AMK: "javascript_adams",
    Company.RN: "javascript_retnment",
}


class Events:
    def __init__(self, company_id: str, db: DB, model_training: bool = False):
        self.company_id = company_id
        self.db = db
        self.df = pd.DataFrame()
        self.model_training = model_training

    def read_from_db(self) -> pd.DataFrame:
        logger.info("Get events data from database...")
        with Path.open(SQL_DIR / "events.sql") as f:
            events = self.db.read_data(f.read().format(SCHEMAS[self.company_id]))

        return events

    def load(self, reload_data: bool = False) -> pd.DataFrame:
        """Get events data for company

        Args:
            company_id (str): company id
            db (DB): database connection

        Returns:
            pd.DataFrame: dataframe of events data
        """
        if self.df.empty or reload_data:
            self.df = self.read_from_db()

        self.df["account_event"] = self.df["event_text"].isin(ACCOUNT_MANAGEMENT)
        self.df["normal_event"] = self.df["event_text"].isin(NORMAL_ACTIVITIES)
        self.df["status_change"] = self.df["event_text"].isin(STATUS_CHANGE)

    def get_features_for_snapshot(
        self,
        snapshot_date: datetime,
        forecast_weeks: int = 4,
        events_last_n_weeks: int = 4,
        average_last_n_weeks: int = 10,
    ) -> pd.DataFrame:
        if self.df.empty:
            return self.df

        # Get data for snapshot
        snapshot_df = self.df.loc[self.df.delivery_date <= snapshot_date]

        date_event_last_n_weeks = snapshot_date + pd.DateOffset(weeks=-events_last_n_weeks)
        date_average_last_n_week = snapshot_date + pd.DateOffset(weeks=-average_last_n_weeks)
        date_last_week = snapshot_date + +pd.DateOffset(weeks=-1)

        df_normal_events_features = self.get_features_for_event_type(
            snapshot_df,
            "normal",
            date_event_last_n_weeks,
            date_average_last_n_week,
            date_last_week,
        )
        df_account_events_features = self.get_features_for_event_type(
            snapshot_df,
            "account",
            date_event_last_n_weeks,
            date_average_last_n_week,
            date_last_week,
        )
        df_status = snapshot_df[snapshot_df["status_change"]]

        # Get forecast status
        forecast_date = snapshot_date + pd.DateOffset(weeks=forecast_weeks)
        df_forecast_status = self.get_status_at_date(df_status=df_status, date=forecast_date).reset_index(
            name="forecast_status",
        )

        # Get current status
        df_current_status = self.get_status_at_date(df_status=df_status, date=snapshot_date).reset_index(
            name="snapshot_status",
        )

        df_event_features = df_normal_events_features.join(df_account_events_features, on="agreement_id", how="left")
        df_event_features = df_event_features.join(df_current_status, on="agreement_id", how="left")
        df_event_features = df_event_features.join(df_forecast_status, on="agreement_id", how="left")

        return df_event_features

    def get_status_at_date(self, df_status: pd.DataFrame, date: datetime) -> pd.DataFrame:
        """Returns status at date

        Args:
            df_status (pd.DataFrame): dataframe of status changes
            date (datetime): last date we want to check

        Returns:
            pd.DataFrame: _description_
        """
        df = df_status[df_status.timestamp <= date]
        max_snapshot_date = df.timestamp.max()
        df = df.loc[df.timestamp == max_snapshot_date]

        if df.empty:
            return df

        df = df.groupby("agreement_id")["event_text"].apply(lambda x: parse_user_status(x["event_text"].iloc[0]))
        return df

    def get_features_for_event_type(
        self,
        snapshot_df: pd.DataFrame,
        event_type: str,
        date_last_n_weeks: datetime,
        date_average_last_n_week: datetime,
        date_last_week: datetime,
    ) -> pd.DataFrame:
        """Returns features for event type
        - total activity
        - number of activities in last N weeks
        - average number of activities difference

        Args:
            df (pd.DataFrame): dataframe of events, either account or normal types
            date_event_last_n_weeks (datetime): the date of cutoff for the last N weeks
            date_average_last_n_week (datetime): date cutoff to calculate average number of events

        Returns:
            pd.DataFrame: _description_
        """

        df = snapshot_df[snapshot_df[f"{event_type}_event"]]

        df_agreement_features = df.groupby("agreement_id").aggregate(
            total_activities=pd.NamedAgg(column="timestamp", aggfunc="count"),
        )
        df_account_last_n_days = (
            df[df.timestamp >= date_last_n_weeks]
            .groupby("agreement_id")
            .aggregate(number_of_activities_last_N_weeks=pd.NamedAgg(column="timestamp", aggfunc="count"))
        )
        df_activity_current_week = (
            df[df.timestamp >= date_last_week]
            .groupby("agreement_id")
            .aggregate(number_of_activities_last_week=pd.NamedAgg(column="timestamp", aggfunc="count"))
        )

        average_activity_difference = (
            df[df.timestamp >= date_average_last_n_week]
            .groupby("agreement_id")
            .aggregate(
                average_activity_difference=pd.NamedAgg(column="timestamp", aggfunc="count") / date_average_last_n_week,
            )
        )

        df_agreement_features = df_agreement_features.merge(df_activity_current_week, how="left", on="agreement_id")
        df_agreement_features = df_agreement_features.merge(average_activity_difference, how="left", on="agreement_id")
        df_agreement_features = df_agreement_features.merge(df_account_last_n_days, how="left", on="agreement_id")

        return df_agreement_features.rename(
            columns={
                "total_activities": f"total_{event_type}_activities",
                "number_of_activities_last_N_weeks": f"number_of_{event_type}_activities_last_N_weeks",
                "number_of_activities_last_week": f"number_of_{event_type}_activities_last_week",
                "average_activity_difference": f"average_{event_type}_activity_difference",
            },
        )


def parse_user_status(user_message: str, keep_freeze_status: bool = True) -> str:
    """
    Cleaning user message to activated/freezed
    :param user_message: raw user message
    :param keep_freeze_status: If False 'freezed' state will be changed to  Prep._label_text_churned
    :return: user clean message
    """
    status_message = user_message.split("_")[-1]
    if status_message in ("activated", "ordered"):
        return LABEL_TEXT_ACTIVE

    # If the customer changes from active to freeze we will predict a churn
    elif status_message == "freezed" and not keep_freeze_status:
        return LABEL_TEXT_CHURNED

    return status_message
