from datetime import datetime

import pandas as pd
import streamlit as st
from streamlit_app.helpers.data import (
    fetch_existing_feedback,
    filter_data_feedback,
    filter_existing_ids,
)
from streamlit_app.helpers.utils import feedback_clicked

st.set_page_config(page_title="Attribute Scoring", page_icon=":tomato:")
st.title("Feedback")

if not st.session_state["authenticated"]:
    st.error("Please authenticate to use the app.")
    st.stop()

recipe_data = st.session_state["recipe_data"]
weekly_menu_data = st.session_state["weekly_menu_data"]

if "existing_feedback_ids" not in st.session_state:
    st.session_state["existing_feedback_ids"] = fetch_existing_feedback()

select_company = st.sidebar.selectbox("Which companies do you want to look at?", ["GL", "AMK", "LMK", "RT"])
select_year = st.sidebar.selectbox("Which year would you like to look at?", [2024, 2025])
select_week = st.sidebar.selectbox(
    "Which week do you want to look at?", list(range(1, 53)), index=datetime.now().isocalendar()[1] - 1
)

filtered_data = filter_data_feedback(df=weekly_menu_data, companies=select_company, year=select_year, week=select_week)

if len(filtered_data) == 0:
    st.info(f"""🚫 There are no recipes from {select_company} in week {select_week} of {select_year}.
        Feel free to explore other weeks or companies to add more feedback!
        """)

else:
    extra_info = st.toggle("Show recipe information")

    if "feedback_data" not in st.session_state:
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

    clean_data = filter_existing_ids(filtered_data)
    st.session_state["clean_data"] = clean_data

    if len(clean_data) == 0:
        st.info(
            f"""🎉 Great job! You've already provided feedback for all recipes
            from {select_company} in week {select_week} of {select_year}.
            Feel free to explore other weeks or companies to add more feedback!
            """
        )

    st.sidebar.write("")
    st.sidebar.write("")
    if st.sidebar.button("Send feedback!"):
        feedback_clicked(st.session_state["feedback_data"])

    for _, row in st.session_state["clean_data"].iterrows():
        st.write(f"#### {row['recipe_name'].capitalize()}")
        col1, col2 = st.columns([1, 2])

        with col1:
            st.image(row["recipe_image_link"], width=200, caption=f"Recipe ID: {row['recipe_id']}")

        with col2:
            family_key = f"{row['recipe_id']}_family"
            chef_key = f"{row['recipe_id']}_chef"

            family_feedback = st.radio(
                "**Family Friendliness**", ["1", "2", "3", "4", "5"], index=None, key=family_key, horizontal=True
            )
            chef_feedback = st.radio(
                "**Chef's Favoriteness**", ["1", "2", "3", "4", "5"], index=None, key=chef_key, horizontal=True
            )

            feedback_row = {
                "recipe_id": row["recipe_id"],
                "company_id": row["company_id"],
                "menu_year": select_year,
                "menu_week": select_week,
                "family_friendliness_feedback": family_feedback,
                "chef_favoriteness_feedback": chef_feedback,
            }

            existing_feedback_index = st.session_state["feedback_data"][
                (st.session_state["feedback_data"]["recipe_id"] == row["recipe_id"])
                & (st.session_state["feedback_data"]["company_id"] == row["company_id"])
                & (st.session_state["feedback_data"]["menu_year"] == select_year)
                & (st.session_state["feedback_data"]["menu_week"] == select_week)
            ].index

            if existing_feedback_index.any():
                st.session_state["feedback_data"].loc[
                    existing_feedback_index, ["family_friendliness_feedback", "chef_favoriteness_feedback"]
                ] = family_feedback, chef_feedback
            else:
                st.session_state["feedback_data"] = pd.concat(
                    [st.session_state["feedback_data"], pd.DataFrame([feedback_row])], ignore_index=True
                )

            if extra_info:
                st.markdown(f"**Main ingredient:** {row['recipe_main_ingredient_name']}")
                st.markdown(f"**Recipe difficulty:** {row['recipe_difficulty_name'].capitalize()}")
                st.markdown(f"**Average cooking time:** {(row['cooking_time_from'] + row['cooking_time_to']) / 2} min")

        st.markdown("---")
