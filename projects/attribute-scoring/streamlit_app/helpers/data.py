import pandas as pd
from streamlit_app.helpers.config import COMPANY_MAP
from streamlit_app.helpers.db import fetch_existing_feedback


def filter_data_overview(df: pd.DataFrame, company: str, year_week: list[str]) -> pd.DataFrame:
    """Filters data for a specified company and week range."""
    filtered_weekly_menu = df[
        (df["company_id"] == COMPANY_MAP[company])
        & (df.apply(lambda row: (row["menu_year"], row["menu_week"]) in year_week, axis=1))
    ]

    filtered_data = filtered_weekly_menu.dropna()

    return filtered_data


def filter_data_feedback(df: pd.DataFrame, companies: str, year: int, week: int) -> pd.DataFrame:
    """Filters recipes based on company, year, and week."""
    selected_company_id = COMPANY_MAP[companies]
    filtered_weekly_menu = df[
        (df["company_id"] == selected_company_id) & (df["menu_year"] == year) & (df["menu_week"] == week)
    ].dropna()

    filtered_data = filtered_weekly_menu

    return filtered_data


def filter_existing_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Filters out rows with recipe IDs that already have feedback."""
    existing_recipe_ids = fetch_existing_feedback()
    return df[~df["recipe_id"].isin(existing_recipe_ids)]
