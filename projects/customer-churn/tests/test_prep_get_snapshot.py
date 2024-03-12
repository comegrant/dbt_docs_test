import logging

import pandas as pd
import pytest
from customer_churn.dataset.bisnode import Bisnode
from customer_churn.dataset.complaints import Complaints
from customer_churn.dataset.crm_segments import CRMSegments
from customer_churn.dataset.customers import Customers
from customer_churn.dataset.events import Events
from customer_churn.dataset.orders import Orders
from customer_churn.features import Features

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


@pytest.mark.asyncio()
def test_prep_get_snapshot_case_1() -> None:
    # forecast_status = freezed (changed during prediction period)

    snapshot_date = "2020-10-10"
    agreement_id = 112211
    _forecast_weeks = 4
    _buy_history_churn_weeks = 4
    dummy_config = "dummy_config"
    gen = Features(dummy_config)

    customers = Customers()
    events = Events()
    orders = Orders()
    bisnode = Bisnode()
    complaints = Complaints()
    crm = CRMSegments()

    customers.load("src/tests/dataset/snapshot_test/customers.csv")
    orders.load("src/tests/dataset/snapshot_test/orders.csv")
    bisnode.load("src/tests/dataset/snapshot_test/bisnode.csv")
    events.load("src/tests/dataset/snapshot_test/events.csv")
    complaints.load("src/tests/dataset/snapshot_test/complaints.csv")
    crm.load("src/tests/dataset/snapshot_test/crm.csv")

    df_snapshot_cust = customers.get_for_date(snapshot_date=snapshot_date)
    dt_from = pd.to_datetime(snapshot_date) + pd.DateOffset(weeks=_forecast_weeks)
    df_snapshot_events = events.get_for_date(dt_from)
    df_snapshot_orders = orders.get_for_date(dt_from)
    df_snapshot_crm_segments = crm.get_for_date(snapshot_date)
    df_snapshot_complaints = complaints.get_for_date(snapshot_date)
    df_snapshot_bisnode = bisnode.get_for_date(snapshot_date)

    customer = df_snapshot_cust[df_snapshot_cust.agreement_id == agreement_id].iloc[0]
    customer_snapshot = gen.generate_features_for_customer(
        customer,
        snapshot_date=snapshot_date,
        df_events=df_snapshot_events,
        df_orders=df_snapshot_orders,
        forecast_weeks=_forecast_weeks,
        df_complaints=df_snapshot_complaints,
        df_bisnode=df_snapshot_bisnode,
        df_crm_segments=df_snapshot_crm_segments,
        buy_history_churn_weeks=_buy_history_churn_weeks,
        model_training=True,
    )

    assert customer_snapshot
