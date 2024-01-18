import re
import pandas as pd
import logging
import numpy as np
from . import common
from . import datasource as source


class Events(source.Datasource):
    df = pd.DataFrame()

    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """
        :param source_file: Events source file
        :return: events DataFrame
        """
        mult = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")

        if self.filepath is None:
            return pd.DataFrame()

        self.df = pd.read_csv(self.filepath, low_memory=False)
        self.df["timestamp"] = pd.to_datetime(self.df.timestamp, utc=True)
        self.df["timestamp"] = self.df["timestamp"].dt.tz_localize(None)
        self.df["event_text"] = (
            self.df["event_text"]
            .apply(lambda x: mult.sub(r"_\1", x).replace(" ", "_"))
            .str.lower()
        )
        print("Total events: " + str(self.df.shape[0]))

    def get_for_date(self, snapshot_date):
        if self.df.empty:
            return self.df
        return self.df.loc[self.df.timestamp <= snapshot_date]

    @staticmethod
    def parse_user_status(user_message, keep_freeze_status=True):
        """
        Cleaning user message to activated/freezed
        :param user_message: raw user message
        :param keep_freeze_status: If False 'freezed' state will be changed to  Prep._label_text_churned
        :return: user clean message
        """
        status_message = user_message.split("_")[-1]
        if status_message == "activated" or status_message == "ordered":
            return common.label_text_active
        # If the customer changes from active to freeze we will predict a churn
        elif status_message == "freezed" and not keep_freeze_status:
            return common.label_text_churned

        return status_message

    def get_customer_events(
        self,
        df_events,
        agreement_id,
        snapshot_date,
        forecast_weeks=4,
        events_last_n_weeks=4,
        average_last_n_weeks=10,
    ):
        """
        :param df_events:
        :param agreement_id:
        :param snapshot_date:
        :param forecast_weeks
        :param events_last_n_weeks: number of events last N weeks
        :param average_last_n_weeks: number of weeks to calculate average on
        :return:
        """
        if self.df.empty:
            return self.df
        _default_start_status = "active"

        statuses = ["change_status__freezed", "change_status__activated"]

        normal_activities = [
            "week_previewed",
            "week_breakfast_previewed",
            "week_planned",
            "valgfri_plan_next_week",
            "change_menu",
            "rated_recipe",
            "_product_added",
            "_product_viewed",
        ]

        account_management = [
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

        # Default values
        dict_out = {
            "agreement_id": agreement_id,
            "total_normal_activities": 0,
            "total_account_activities": 0,
            "number_of_normal_activities_last_N_weeks": 0,
            "number_of_account_activities_last_N_weeks": 0,
            "average_normal_activity_difference": 0,
            "average_account_activity_difference": 0,
            "snapshot_status": _default_start_status,
            "forecast_status": _default_start_status,
        }

        # Get only records of interest
        df = df_events[df_events.agreement_id == agreement_id]
        if df.empty:
            return pd.DataFrame([dict_out])

        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(weeks=forecast_weeks)

        # Get only status_change statuses
        df_status = df[df.event_text.isin(statuses)]
        # Get only normal and account activity
        df_normal = df[df.event_text.isin(normal_activities)]
        df_account = df[df.event_text.isin(account_management)]

        # Getting snapshot status
        df_snap = df_status.loc[df.timestamp <= snapshot_date]
        max_snapshot_date = df_snap.timestamp.max()
        df_snap = df_snap.loc[df_snap.timestamp == max_snapshot_date]

        if not df_snap.empty:
            user_status = df_snap.iloc[0]["event_text"]
            dict_out["snapshot_status"] = self.parse_user_status(
                df_snap.iloc[0]["event_text"], keep_freeze_status=True
            )

        # Getting forecast status
        df_forecast = df_status.loc[df_status.timestamp <= dt_from]
        max_snapshot_date = df_forecast.timestamp.max()
        df_forecast = df_forecast.loc[df_forecast.timestamp == max_snapshot_date]

        if not df_forecast.empty:
            dict_out["forecast_status"] = self.parse_user_status(
                df_forecast.iloc[0]["event_text"], keep_freeze_status=True
            )

        # Getting number of normal activities
        df_normal = df_normal.loc[df_normal.timestamp <= snapshot_date]
        cust_events = df_normal.event_text.value_counts()
        normal_event_sum = cust_events.sum()
        dict_out["total_normal_activities"] = normal_event_sum

        # Getting number of account activities
        df_account = df_account.loc[df_account.timestamp <= snapshot_date]
        cust_events = df_account.event_text.value_counts()
        account_event_sum = cust_events.sum()
        dict_out["total_account_activities"] = account_event_sum

        # Getting last N weeks of normal activities
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
            weeks=-events_last_n_weeks
        )
        last_n_normal_activities = df_normal[df_normal.timestamp >= dt_from].shape[0]
        dict_out["number_of_normal_activities_last_N_weeks"] = last_n_normal_activities

        # Getting last N weeks of account activities
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
            weeks=-events_last_n_weeks
        )
        last_n_account_activities = df_account[df_account.timestamp >= dt_from].shape[0]
        dict_out[
            "number_of_account_activities_last_N_weeks"
        ] = last_n_account_activities

        # Getting difference from average number of events last N weeks to this weeks number of events
        # Get last n week average
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
            weeks=-average_last_n_weeks
        )
        last_n_normal_activities = df_normal[df_normal.timestamp >= dt_from].shape[0]
        average_normal_activities_n_weeks = (
            last_n_normal_activities / average_last_n_weeks
        )
        # Get number of events for this week
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(weeks=-1)
        number_of_normal_activities_current_week = df_normal[
            df_normal.timestamp >= dt_from
        ].shape[0]
        # Get difference
        diff_avg = (
            average_normal_activities_n_weeks - number_of_normal_activities_current_week
        )
        dict_out["average_normal_activity_difference"] = diff_avg

        # Getting difference from average number of events last N weeks to this weeks number of events
        # Get last n week average
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(
            weeks=-average_last_n_weeks
        )
        last_n_account_activities = df_account[df_account.timestamp >= dt_from].shape[0]
        average_account_activities_n_weeks = (
            last_n_account_activities / average_last_n_weeks
        )
        # Get number of events for this week
        dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(weeks=-1)
        number_of_account_activities_current_week = df_account[
            df_account.timestamp >= dt_from
        ].shape[0]
        # Get difference
        diff_avg = (
            average_account_activities_n_weeks
            - number_of_account_activities_current_week
        )
        dict_out["average_account_activity_difference"] = diff_avg

        return pd.DataFrame([dict_out])
