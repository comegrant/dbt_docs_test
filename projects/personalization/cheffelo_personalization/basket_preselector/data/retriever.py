from typing import Dict

from lmkgroup_ds_utils.db.connector import DB

from .customers import CustomerData
from .menu import get_menu_preference_rules, get_menu_products_with_preferences
from .models.utils import Yearweek
from .rec_engine import get_recommendation_score_for_company_id_and_yearweek


def get_data_from_db(company_id: str, yearweek: Yearweek, db: DB, run_config: Dict):
    """_summary_

    Args:
        company_id (str): _description_
        yearweek (Yearweek): _description_

    Returns:
        _type_: _description_
    """

    customer_data = CustomerData(company_id=company_id, db=db)

    df_customers = customer_data.get_customer_information_for_company_id()

    df_menu = get_menu_products_with_preferences(
        company_id=company_id, yearweek=yearweek, db=db
    )

    df_preference_rules = None
    if run_config["rules"]["preference_rules"]:
        df_preference_rules = get_menu_preference_rules(db=db)

    df_recommendations = None
    if run_config["ranking"]["rec_engine"]:
        df_recommendations = get_recommendation_score_for_company_id_and_yearweek(
            company_id=company_id, year=yearweek.year, week=yearweek.week, db=db
        )

    df_quarantined_dishes = None
    if run_config["ranking"]["quarantine"]:
        df_quarantined_dishes = customer_data.get_customer_quarantined_dishes(
            start_year=yearweek.year,
            start_week=yearweek.week,
            num_quarantine_weeks=run_config["ranking"]["num_quarantine_weeks"],
        )

    return (
        df_customers,
        df_menu,
        df_recommendations,
        df_preference_rules,
        df_quarantined_dishes,
    )
