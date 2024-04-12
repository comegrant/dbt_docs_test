from aligned import Float, Int32, String, Timestamp, feature_view

from data_contracts.contacts import Contacts
from data_contracts.sources import adb, folder

contacts = [Contacts.thomassve().markdown(), Contacts.niladri().markdown()]


ORDERS_SQL = """
    SELECT
        agreement_id
    ,	company_name
    ,	order_id
    ,	delivery_date
    ,	delivery_year
    ,	delivery_week
    ,	net_revenue_ex_vat
    ,	gross_revenue_ex_vat
    FROM mb.orders
"""


@feature_view(
    name="orders",
    description="The order information",
    materialized_source=folder.parquet_at("orders.parquet"),
    source=adb.fetch(ORDERS_SQL),
    contacts=contacts,
)
class Customers:
    agreement_id = Int32().as_entity()
    company_name = String()
    order_id = Int32.as_entity()
    delivery_date = Timestamp()
    delivery_year = Int32()
    delivery_week = Int32()
    net_revenue_ex_vat = Float()
    gross_revenue_ex_vat = Float()


class CustomersAggregated:
    agreement_id = Int32().as_entity()
    snapshot_date = Timestamp()
    year = Int32()
    month = Int32()
    week = Int32()
    number_of_total_orders = Int32()
    number_of_forecast_orders = Int32()
    weeks_since_last_delivery = Int32()
