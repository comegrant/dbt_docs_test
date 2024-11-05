from aligned import (
    Bool,
    Int32,
    String,
    Timestamp,
    feature_view,
)
from aligned.sources.in_mem_source import InMemorySource

from data_contracts.sources import adb, materialized_data

user_subscription = """SELECT
    ba.agreement_id,
    ba.company_id,
    ba.status,
    ba.start_date,
    ba.method_code,
    ba.created_at,
    ba.updated_at,
    bab.delivery_week_type,
    bab.is_active,
    bab.timeblock,
    babp.subscribed_product_variation_id
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON ba.agreement_id = bab.agreement_id
INNER JOIN cms.billing_agreement_basket_product babp ON bab.id = babp.billing_agreement_basket_id"""

@feature_view(
    name="user_subscription",
    source=adb.fetch(user_subscription),
    materialized_source=materialized_data.parquet_at("user_subscription.parquet"),
)
class UserSubscription:
    agreement_id = Int32().as_entity()

    company_id = String()
    status = Int32()
    start_date = Timestamp()

    method_code = String()

    created_at = Timestamp()
    updated_at = Timestamp()

    delivery_week_type = Int32()
    is_active = Bool()
    timeblock = Int32()

    subscribed_product_variation_id = String()

@feature_view(
    name="user_completed_onesub_quiz",
    source=InMemorySource.empty()
)
class UserCompletedQuiz:
    agreement_id = Int32().as_entity()
    has_completed_quiz = Bool()
