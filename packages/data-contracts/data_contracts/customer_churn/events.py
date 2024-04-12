from aligned import EventTimestamp, Int32, String, feature_view

from data_contracts.sources import adb

DEFAULT_START_STATUS = "active"


@feature_view(
    sources=[],
    tags=["customer_churn"],
)
class CustomerEventsAggregated:
    agreement_id: String
    total_normal_activities: Int32 = 0
    total_account_activities: Int32 = 0
    number_of_normal_activities_last_n_weeks: Int32 = 0
    number_of_account_activities_last_n_weeks: Int32 = 0
    average_normal_activity_difference: Int32 = 0
    average_account_activity_difference: Int32 = 0
    snapshot_status: String = DEFAULT_START_STATUS
    forecast_status: String = DEFAULT_START_STATUS


SQL_EVENTS = """
    SELECT
        agreement_id
        ,   event_text
        ,   timestamp
    FROM {schema}.tracks_churn_view
"""


@feature_view(
    sources=[adb.fetch(SQL_EVENTS)],
    tags=["customer_churn"],
)
class CustomerEvents:
    agreement_id: String
    event_text: String
    timestamp: EventTimestamp
