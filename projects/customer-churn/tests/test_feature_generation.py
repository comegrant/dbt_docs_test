import logging

from .data import (
    generate_bisnode_data,
    generate_complaints_data,
    generate_crm_data,
    generate_customer_data,
    generate_events_data,
    generate_orders_data,
)

logger = logging.getLogger(__name__)


def test_feature_generation() -> None:
    df_bisnode = generate_bisnode_data()
    df_customer = generate_customer_data()
    df_complaints = generate_complaints_data()
    df_events = generate_events_data()
    df_orders = generate_orders_data()
    df_crm = generate_crm_data()

    logger.info(df_bisnode)
    logger.info(df_customer)
    logger.info(df_complaints)
    logger.info(df_events)
    logger.info(df_orders)
    logger.info(df_crm)
