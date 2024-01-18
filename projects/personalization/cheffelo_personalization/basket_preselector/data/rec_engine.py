import logging

import pandas as pd
from lmkgroup_ds_utils.db.connector import DB

from cheffelo_personalization.basket_preselector.utils.paths import SQL_REC_ENGINE_DIR

logger = logging.getLogger(__name__)


def get_recommendation_score_for_company_id_and_yearweek(
    company_id: str, year: int, week: int, db: DB
) -> pd.DataFrame:
    """Gets all active customers for a given company id

    Args:
        company_id (str): id of company to fetch active agreement ids
        read_db (DB): database connection

    Returns:
        DataFrame: pandas dataframe of customer information
    """
    logger.info(
        "Reading recommendation scores for company %s and year %s week %s",
        company_id,
        year,
        week,
    )
    with open(SQL_REC_ENGINE_DIR / "get_recommendations_for_company_id.sql") as f:
        df = db.read_data(
            f.read().format(
                company_id=company_id,
                year=year,
                week=week,
            )
        )

    logger.info("Fetched total %s recommendations", len(df))
    return df
