from datetime import datetime, timedelta
from math import trunc

import pandas as pd
import streamlit as st
from streamlit_app.helpers.db import fetch_existing_feedback, send_feedback_to_db


def user_friendly_score(score: float) -> str:
    """Converts a numeric score into a user-friendly descriptive rating."""
    if score >= 0.8:
        return f"Very High ({trunc(score * 10)}/10)"
    elif score >= 0.6:
        return f"High ({trunc(score * 10)}/10)"
    elif score >= 0.4:
        return f"Medium ({trunc(score * 10)}/10)"
    elif score >= 0.2:
        return f"Low ({trunc(score * 10)}/10)"
    else:
        return f"Very Low ({trunc(score * 10)}/10)"


def get_weeks() -> list:
    """Generates a list of 7 weeks in 'YYYY-WW' format."""
    today = datetime.today()
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday + 7 if days_since_monday < 7 else 0)

    week_list = []
    for i in range(7):  # last week + 6 weeks
        current_week = last_monday + timedelta(weeks=i)
        year, week, _ = current_week.isocalendar()
        week_list.append(f"{year}-{week:02d}")

    return week_list


def get_default_week() -> str:
    """Returns the current ISO year and week in 'YYYY-WW' format."""
    today = datetime.today()
    year, week, _ = today.isocalendar()
    return f"{year}-{week:02d}"


def feedback_clicked(df: pd.DataFrame) -> None:
    """Processes and submits feedback and refreshes app."""
    df = df.dropna(subset=["family_friendliness_feedback", "chef_favoriteness_feedback"], how="all")
    st.toast("Thank you for your feedback!", icon="🧡")
    send_feedback_to_db(df)
    st.session_state["feedback_data"] = pd.DataFrame(
        columns=[
            "recipe_id",
            "company_id",
            "menu_year",
            "menu_week",
            "family_friendliness_feedback",
            "chef_favoriteness_feedback",
        ]
    )
    st.session_state["existing_feedback_ids"] = fetch_existing_feedback()
    st.rerun()
