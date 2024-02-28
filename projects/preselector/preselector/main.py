import logging
import time

import pandas as pd

from preselector.data.models.customer import (
    PreselectorCustomer,
    PreselectorResult,
    RunConfig,
)
from preselector.rules.engine import run_preselector_engine

logger = logging.getLogger(__name__)


def run_preselector_for(
    customer: PreselectorCustomer,
    run_config: RunConfig,
    df_flex_products: pd.DataFrame,
    df_recommendations: pd.DataFrame | None = None,
    df_preference_rules: pd.DataFrame | None = None,
    df_quarantined_dishes: pd.DataFrame | None = None,
) -> PreselectorResult | Exception:
    """Run preselector for a single customer

    Args:
        customer (pd.Series): Series of customer information
        df_flex_products (pd.DataFrame): All possible dishes for the week

    Returns:
        Dict: dictionary of selected dishes and debug summary
    """
    df_rec = pd.DataFrame()
    if (
        run_config.should_rank_using_rec_engine
        and isinstance(
            df_recommendations,
            pd.DataFrame,
        )
        and not df_recommendations.empty
    ):
        df_rec = df_recommendations.loc[df_recommendations["agreement_id"] == customer.agreement_id]

    df_quarantined_dishes_for_customer = pd.DataFrame()
    if (
        run_config.should_rank_with_quarantine
        and isinstance(
            df_quarantined_dishes,
            pd.DataFrame,
        )
        and not df_quarantined_dishes.empty
    ):
        df_quarantined_dishes_for_customer = df_quarantined_dishes.loc[
            df_quarantined_dishes["agreement_id"] == customer.agreement_id
        ]

    if df_preference_rules is None:
        df_preference_rules = pd.DataFrame()

    start_time = time.time()
    result = run_preselector_engine(
        customer,
        df_flex_products,
        df_rec,
        df_preference_rules,
        df_quarantined_dishes_for_customer,
        run_config,
    )
    if isinstance(result, Exception):
        return result

    selected_dishes, debug_summary = result

    debug_summary["preselector_total_time"] = time.time() - start_time

    return PreselectorResult(
        agreement_id=customer.agreement_id,
        main_recipe_ids=selected_dishes["main_recipe_id"].tolist(),
        debug_summary=debug_summary,
    )
