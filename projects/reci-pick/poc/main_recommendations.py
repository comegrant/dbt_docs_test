import streamlit as st
from poc.section_display_recommendations import section_display_recommendations
from poc.section_read_data import read_data
from poc.section_set_parameters import section_set_params

# Display images in a carousel
st.set_page_config(layout="wide")
st.title("Your menu recommendations")

if "has_data" not in st.session_state:
    st.session_state["has_data"] = False

company_code, agreement_id, menu_weeks = section_set_params()
if agreement_id is not None:
    agreement_id = int(agreement_id)
if st.button("I want to see my recommendations!"):
    df = read_data(company_code=company_code)
    st.session_state["has_data"] = True

    if st.session_state["has_data"]:
        df_user_week = df[(df["billing_agreement_id"] == agreement_id) & (df["menu_yyyyww"].isin(menu_weeks))]
        if df_user_week.shape[0] < 1:
            st.write("Could not find your recommendations 😰 ")
        else:
            section_display_recommendations(df_recommendations=df_user_week, num_image_per_row=5)
