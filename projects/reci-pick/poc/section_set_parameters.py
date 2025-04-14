import streamlit as st


def section_set_params() -> tuple[str, any, list[int]]:
    col1, col2, col3 = st.columns(3)
    with col1:
        brand = st.selectbox(
            "Which brand do you subscribe to?",
            (
                "Adams Matkasse",
                "Godtlevert",
                "Linas Matkasse",
                "RetNemt",
            ),
        )
        company_code = None
        if brand == "Adams Matkasse":
            company_code = "AMK"
        elif brand == "Godtlevert":
            company_code = "GL"
        elif brand == "Linas Matkasse":
            company_code = "LMK"
        elif brand == "RetNemt":
            company_code = "RT"
    with col2:
        agreement_id = st.text_input(
            "Your agreement id",
            value=None,
        )

    with col3:
        menu_weeks = st.multiselect(
            "Which menu week(s)?",
            options=[202513, 202514, 202515, 202516, 202517, 202518],
            default=[202513],
        )
    return company_code, agreement_id, menu_weeks
