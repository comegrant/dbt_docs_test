from aligned import Int32, String, Timestamp, feature_view

from data_contracts.contacts import Contacts
from data_contracts.sources import adb, folder

contacts = [Contacts.thomassve().markdown(), Contacts.niladri().markdown()]


CUSTOMERS_SQL = """
    SELECT
        agreement_id
    ,	agreement_status
    ,	agreement_start_date
    ,	agreement_start_year
    ,	agreement_start_week
    ,	agreement_first_delivery_year
    ,	agreement_first_delivery_week
    ,   agreement_creation_date
    ,   agreement_first_delivery_date
    ,   last_delivery_date
    ,   next_estimated_delivery_date

    FROM mb.customers
    WHERE agreement_status_id != 40
"""


@feature_view(
    name="customers",
    description="The customer information",
    materialized_source=folder.parquet_at("customers.parquet"),
    source=adb.fetch(CUSTOMERS_SQL),
    contacts=contacts,
)
class Customers:
    agreement_id = Int32().as_entity()
    agreement_status = String()
    agreement_start_date = Timestamp()
    agreement_start_year = Int32()
    agreement_start_week = Int32()
    agreement_first_delivery_year = Int32()
    agreement_first_delivery_week = Int32()
    agreement_creation_date = Timestamp()
    agreement_first_delivery_date = Timestamp()
    last_delivery_date = Timestamp()
    next_estimated_delivery_date = Timestamp()


@feature_view(
    name="customers",
    description="The customer information",
    materialized_source=folder.parquet_at("customers.parquet"),
    source=adb.fetch(CUSTOMERS_SQL),
    contacts=contacts,
)
class CustomersAggregated:
    agreement_id = Int32().as_entity()
    agreement_start_year = Int32()
    agreement_start_week = Int32()
    agreement_first_delivery_year = Int32()
    agreement_first_delivery_week = Int32()
    agreement_status = String()
    agreement_start_date = Timestamp()
