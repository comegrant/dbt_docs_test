import streamlit as st
import pandas as pd
import json
from menu_optimiser.common import RequestMenu
from menu_optimiser.old_mop.optimization import generate_menu_companies
from streamlit_app.helpers.utils import (
    get_new_recipe_data,
    map_new_taxonomies,
)
from constants.companies import get_company_by_id
from menu_optimiser.menu_solver import generate_menu

# from streamlit_app.helpers.config import VALID_TAXONOMIES
from streamlit_app.helpers.utils import (
    get_extra_columns_for_old_menu,
    create_menu_with_ingredients,
)

from streamlit_helper import setup_streamlit
from menu_optimiser.db import get_recipe_ingredients_data

st.set_page_config(page_title="Menu Optimiser", page_icon=":bento:")


async def preprocess_menus(df_new, df_old):
    df_old = await get_extra_columns_for_old_menu(df_old)

    df_old = await get_new_recipe_data(df_old)
    df_old = map_new_taxonomies(df_old, "cuisine")
    df_old = map_new_taxonomies(df_old, "dish_type")
    df_old = map_new_taxonomies(
        df_old,
        "taxonomies",
        list_of_taxonomies=st.session_state["VALID_TAXONOMIES"],
        keep_ids=True,
    )

    df_new = await get_new_recipe_data(df_new)
    df_new = map_new_taxonomies(df_new, "cuisine")
    df_new = map_new_taxonomies(df_new, "dish_type")
    df_new = map_new_taxonomies(
        df_new,
        "taxonomies",
        list_of_taxonomies=st.session_state["VALID_TAXONOMIES"],
        keep_ids=True,
    )

    # if there are nan values within lists within the columns that are list, fill with "unknown"
    # TODO: find better way than this hacky
    def fill_unknown(x):
        try:
            # If x is a list, fill any nan/None in the list with "unknown"
            if isinstance(x, list):
                return ["unknown" if pd.isna(i) else i for i in x]
            # If x is a scalar, check for nan/None
            return ["unknown"] if pd.isna(x) else [x]
        except Exception:
            return x

    # only do this for certain columns, e.g. cuisine, dish_type, main_protein, main_carb
    columns_to_fill = [
        "cuisine",
        "dish_type",
        "main_protein",
        "main_carb",
        "protein_processing",
    ]
    df_old[columns_to_fill] = df_old[columns_to_fill].applymap(fill_unknown)
    df_new[columns_to_fill] = df_new[columns_to_fill].applymap(fill_unknown)
    return df_new, df_old


@setup_streamlit(datadog_config=None, datadog_metrics_config=None)
async def main():
    st.title("Menu Optimiser")

    st.header("📁 Data Upload")
    uploaded_file = st.file_uploader(
        "Upload your business rule input to generate a menu (JSON)",
        type=["json"],
        help="JSON file with business input",
    )

    if uploaded_file is None:
        if st.session_state.get("company") is not None:
            st.info(
                f"Successfully generated menu for {st.session_state.get('company')}"
            )
        return

    if not (
        "new_menu_data" not in st.session_state
        or "old_menu_data" not in st.session_state
        or "company" not in st.session_state
        or st.session_state.get("last_uploaded_filename") != uploaded_file.name
    ):
        return

    try:
        with st.spinner("Loading data..."):
            data = json.load(uploaded_file)
            payload = RequestMenu.model_validate(data)

            st.session_state["VALID_TAXONOMIES"] = [
                taxonomy.taxonomy_id for taxonomy in payload.companies[0].taxonomies
            ]

        with st.spinner("Generating new menu..."):
            problem, df_new, error = await generate_menu(payload)

        with st.spinner("Generating old menu..."):
            _, df_old = await generate_menu_companies(payload)

        with st.spinner("Fetching additional data for visualisations..."):
            df_new, df_old = await preprocess_menus(df_new, df_old)

        with st.spinner("Getting recipe ingredients..."):
            df_recipe_ingredients = await get_recipe_ingredients_data()
            df_new, df_new_ingredients = create_menu_with_ingredients(
                df_new, df_recipe_ingredients
            )
            df_old, df_old_ingredients = create_menu_with_ingredients(
                df_old, df_recipe_ingredients
            )

        st.session_state["payload"] = payload
        st.session_state["new_menu_data"] = df_new
        st.session_state["old_menu_data"] = df_old
        st.session_state["new_menu_ingredients"] = df_new_ingredients
        st.session_state["old_menu_ingredients"] = df_old_ingredients
        st.session_state["last_uploaded_filename"] = uploaded_file.name
        st.session_state["company"] = get_company_by_id(
            payload.companies[0].company_id.upper()
        ).company_name
        st.session_state["error"] = error
        st.session_state["status"] = problem.status

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info(
            "Please ensure your JSON file has the required columns and proper formatting."
        )


if __name__ == "__main__":
    main()  # pyright: ignore
