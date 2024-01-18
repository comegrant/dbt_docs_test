import logging

from ..gen import Gen
from ..utils.dataprep.orders import Orders
from ..utils.dataprep.events import Events
from ..utils.dataprep.complaints import Complaints
from ..utils.dataprep.customers import Customers
from ..utils.dataprep.crm_segments import CRM_segments
from ..utils.dataprep.bisnode import Bisnode
import pandas as pd

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def test_prep_get_snapshot_case_1():
    # snapshot_state = active
    # forecast_status = freezed (changed during prediction period)

    snapshot_date = "2020-10-10"
    _forecast_weeks = 4
    _buy_history_churn_weeks = 4
    dummy_config = "dummy_config"
    gen = Gen(dummy_config)

    customers = Customers()
    events = Events()
    orders = Orders()
    bisnode = Bisnode()
    complaints = Complaints()
    crm = CRM_segments()

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

    customer = df_snapshot_cust[df_snapshot_cust.agreement_id == 112211].iloc[0]
    customer_snapshot = gen.get_customer_snapshot(
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

    assert customer_snapshot.iloc[0]["snapshot_status"] == "active"
    assert customer_snapshot.iloc[0]["forecast_status"] == "freezed"
    assert customer_snapshot.iloc[0]["week"] == 41
    assert customer_snapshot.iloc[0]["month"] == 10
    assert customer_snapshot.iloc[0]["year"] == 2020
    assert customer_snapshot.iloc[0]["customer_since_weeks"] == 25
    assert customer_snapshot.iloc[0]["weeks_since_last_delivery"] == 3
    assert customer_snapshot.iloc[0]["sub_segment_name"] == "super_loyal"
    assert customer_snapshot.iloc[0]["confidence_level"] == "green"
    assert int(customer_snapshot.iloc[0]["children_probability"]) == 30
    assert customer_snapshot.iloc[0]["weeks_since_last_complaint"] == -1
