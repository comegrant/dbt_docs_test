from aligned import Int32, String, Timestamp, feature_view

from data_contracts.sources import adb

SQL_COMPLAINTS = """
    SELECT
        agreement_id
    ,	delivery_year
    ,	delivery_week
    ,	category
    ,	registration_date
    FROM mb.complaints
"""


@feature_view(
    sources=[adb.fetch(SQL_COMPLAINTS)],
    tags=["customer_churn"],
)
class Complaints:
    agreement_id = String()
    delivery_year = Int32()
    delivery_week = Int32()
    category = String()
    registration_date = Timestamp()


SQL_COMPLAINTS_AGGREGATED = """
    SELECT
"""


@feature_view(
    sources=[adb.fetch(SQL_COMPLAINTS_AGGREGATED)],
    tags=["customer_churn"],
)
class ComplaintsAggregated:
    agreement_id = String()
    total_complaints = Int32()
    category = String()
    weeks_since_last_complaint = Int32()
    number_of_complaints_last_n_weeks = Int32()
