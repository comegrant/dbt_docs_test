import streamlit as st
import json
from menu_optimiser.common import RequestMenu
import asyncio
from menu_optimiser.optimization import generate_menu_companies
from streamlit_app.helpers.utils import get_recipe_data, map_taxonomies


st.set_page_config(page_title="Menu Optimiser", page_icon=":bento:")
st.title("Menu Optimiser")
st.write("This is a menu optimiser app.")

if st.button("🔁 Reset app"):
    st.session_state.clear()
    st.rerun()

for key in [
    "business_input",
    "payload",
    "response",
    "company",
    "recipes",
    "week",
    "year",
    "status_msg",
    "recipe_data",
]:
    if key not in st.session_state:
        st.session_state[key] = None


uploaded_file = st.file_uploader("Upload or replace JSON file", type=["json"])

if uploaded_file:
    try:
        data = json.load(uploaded_file)
        st.session_state["business_input"] = data
        st.session_state["payload"] = RequestMenu.model_validate(data)

        for key in [
            "response",
            "company",
            "recipes",
            "week",
            "year",
            "status_msg",
            "recipe_data",
        ]:
            st.session_state[key] = None

        st.success("File loaded successfully.")
    except Exception as e:
        st.error(f"Failed to load file: {e}")

# button to generate menu and recipe data
if st.session_state["payload"] is not None:
    if st.button("Generate Menu"):
        with st.spinner("Generating menu..."):
            payload = st.session_state["payload"]
            try:
                response = asyncio.run(generate_menu_companies(payload))
                st.session_state["company"] = response.companies[0]
                st.session_state["recipes"] = [
                    r.recipe_id for r in st.session_state["company"].recipes
                ]
                st.session_state["week"] = response.week
                st.session_state["year"] = response.year
                st.session_state["status_msg"] = response.status_msg

                # get additional recipe data
                data = asyncio.run(
                    get_recipe_data(
                        st.session_state["recipes"],
                        st.session_state["company"].company_id,
                    )
                )

                # map taxonomies to recipe data
                relevant_taxonomies = [
                    t.taxonomy_id for t in st.session_state["company"].taxonomies
                ]
                data_with_taxonomies = map_taxonomies(data, relevant_taxonomies)

                st.session_state["recipe_data"] = data_with_taxonomies
                st.session_state["response"] = response

                st.success("Menu generated successfully!")
            except Exception as e:
                st.error(f"Error generating menu: {str(e)}")
