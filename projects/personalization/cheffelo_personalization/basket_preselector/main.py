import datetime
import logging
import os
import time
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from .data.processor import process_api_data, process_raw_data
from .pipeline import get_run_configs
from .rules.engine import run_preselector_engine
from .utils.constants import LOG_TO_FILE
from .utils.paths import LOG_DIR

if LOG_TO_FILE:
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logging.basicConfig(
        level=logging.DEBUG,
        filename=LOG_DIR
        / "preselector_run_{}.log".format(
            datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        ),
    )
logger = logging.getLogger(__name__)


def run_preselector_batch(
    df_customers: pd.DataFrame,
    df_flex_products: pd.DataFrame,
    df_recommendations: Optional[pd.DataFrame] = None,
    df_preference_rules: Optional[pd.DataFrame] = None,
    df_quarantined_dishes: Optional[pd.DataFrame] = None,
    run_config: Optional[Dict] = None,
    verbose: bool = False,
):
    # For each of the customers, generate their preselected dishes
    logger.info("iterate over each customer and select their preselected dishes")
    if not run_config:
        run_config = get_run_configs()

    output = []
    for _, customer in tqdm(
        df_customers.iterrows(), total=len(df_customers), disable=not verbose
    ):
        try:
            df_rec = pd.DataFrame()
            if run_config["ranking"]["rec_engine"] and isinstance(
                df_recommendations, pd.DataFrame
            ):
                if not df_recommendations.empty:
                    df_rec = df_recommendations.loc[
                        df_recommendations["agreement_id"] == customer["agreement_id"]
                    ]

            df_quarantined_dishes_for_customer = pd.DataFrame()
            if run_config["ranking"]["quarantine"] and isinstance(
                df_quarantined_dishes, pd.DataFrame
            ):
                if not df_quarantined_dishes.empty:
                    df_quarantined_dishes_for_customer = df_quarantined_dishes.loc[
                        df_quarantined_dishes["agreement_id"]
                        == customer["agreement_id"]
                    ]

            start_time = time.time()
            selected_dishes, debug_summary = run_preselector_engine(
                customer,
                df_flex_products,
                df_rec,
                df_preference_rules,
                df_quarantined_dishes_for_customer,
                run_config,
            )
            debug_summary["preselector_total_time"] = time.time() - start_time

            result = {
                "agreement_id": customer["agreement_id"],
                "selected_dishes": selected_dishes["main_recipe_id"].tolist(),
                "debug_summary": debug_summary,
            }
            output.append(result)

            logger.info(result)
        except Exception:
            logger.exception("Failed to run preselector")
    return output


def run_preselector_batch_api(
    agreements: List,
    product_information: List,
    recipe_information: List,
    recommendation_scores: Optional[List],
    preference_rules: List,
):
    run_config = get_run_configs()
    (
        df_customers,
        df_flex_products,
        df_recommendations,
        df_preference_rules,
    ) = process_api_data(
        agreements,
        product_information,
        recipe_information,
        recommendation_scores,
        preference_rules,
        run_config,
    )
    return run_preselector_batch(
        df_customers, df_flex_products, df_recommendations, df_preference_rules
    )


def run_preselector_batch_locally(
    df_customers: pd.DataFrame,
    df_menu: pd.DataFrame,
    df_recommendations: Optional[pd.DataFrame],
    df_preference_rules: Optional[pd.DataFrame],
    df_quarantined_dishes: Optional[pd.DataFrame],
    run_config: Optional[Dict],
):
    df_customers, df_flex_products, df_recommendations = process_raw_data(
        df_customers, df_menu, df_recommendations, run_config
    )
    return run_preselector_batch(
        df_customers,
        df_flex_products,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
        run_config,
        verbose=True,
    )
