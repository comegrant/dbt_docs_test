from aligned import Int32, Timestamp, feature_view

from data_contracts.contacts import Contacts
from data_contracts.sources import adb, folder

contacts = [Contacts.thomassve().markdown(), Contacts.niladri().markdown()]


SQL_QUERY = """
    SELECT
        agreement_id
    ,	current_delivery_year
    ,	current_delivery_week
    ,	planned_delivery
    FROM analytics.crm_segment_agreement_main_log
    AND sub_segment_name != 'Deleted'   -- temporary as there are no events for deleted users
"""


@feature_view(
    name="crm_segment",
    description="The crm segment information",
    materialized_source=folder.parquet_at("crm_segment.parquet"),
    source=adb.fetch(SQL_QUERY),
    contacts=contacts,
)
class Customers:
    agreement_id = Int32().as_entity()
    planned_delivery = Timestamp()
    current_delivery_year = Int32()
    current_delivery_week = Int32()
