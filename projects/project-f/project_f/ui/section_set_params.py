import streamlit as st
from project_f.data.assumptions.budget_period import budget_periods


def section_set_params() -> tuple[str, int, int, int, str]:
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        brand = st.selectbox(
            "Choose a brand to budget for",
            (
                "Adams Matkasse",
                "Godtlevert",
                "Linas Matkasse",
                "RetNemt",
            ),
        )
    with col2:
        budget_type = st.selectbox("Select a budget type", ("F3", "Budget", "F1", "F2"))
    with col3:
        budget_year_default = max(budget_periods[budget_type].keys())
        budget_year = st.number_input(
            "Budget year",
            min_value=2025,
            value=int(budget_year_default),
        )
    with col4:
        if budget_year in budget_periods[budget_type]:
            budget_start_default = budget_periods[budget_type][budget_year]["start_yyyyww"]
        else:
            budget_start_default = budget_periods[budget_type][budget_year_default]["start_yyyyww"]
        budget_start = st.number_input(
            "Start year week (yyyyww format, i.e., 202501)",
            min_value=202501,
            value=int(budget_start_default),
        )
    with col5:
        if budget_year in budget_periods[budget_type]:
            budget_end_default = budget_periods[budget_type][budget_year]["end_yyyyww"]
        else:
            budget_end_default = budget_periods[budget_type][budget_year_default]["end_yyyyww"]
        budget_end = st.number_input(
            "End year week (yyyyww format, i.e., 202552)",
            min_value=202552,
            value=int(budget_end_default),
        )
    st.write(f"Budgetting for {budget_year} - {budget_type}! Period: {budget_start} - {budget_end}")
    return budget_type, budget_year, budget_start, budget_end, brand
