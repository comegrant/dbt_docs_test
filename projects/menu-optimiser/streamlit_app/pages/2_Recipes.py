import streamlit as st
import pandas as pd
from constants.companies import get_company_by_id
from streamlit_app.helpers.utils import format_as_tags

st.set_page_config(layout="wide")
st.title("Menu Overview")


response = st.session_state["response"]
company = st.session_state["company"]
company_name = get_company_by_id(company.company_id.upper()).company_name
week = st.session_state["week"]
year = st.session_state["year"]
status_msg = st.session_state["status_msg"]

recipe_data = st.session_state["recipe_data"]
recipes = st.session_state["recipes"]

col1, col2 = st.columns([1, 2])

with col1:
    column_or_list = st.segmented_control(
        "Layout:",
        options=["Single View", "Split View"],
        default="Single View",
        selection_mode="single",
    )

with col2:
    sort_direction = st.segmented_control(
        "Sort by:",
        options=["Rating: Low to High", "Rating: High to Low"],
        default="Rating: High to Low",
        selection_mode="single",
    )

if sort_direction:
    ascending = sort_direction == "Rating: Low to High"
    recipe_data = recipe_data.sort_values(by=["average_rating"], ascending=ascending)

if column_or_list == "Single View":
    for _, row in recipe_data.iterrows():
        st.write(f"#### {row['recipe_name'].capitalize()}")
        url = row["pim_link"]

        col1, col2, col3 = st.columns([1, 1, 3])

        with col1:
            try:
                image_link = str(row["image_link"])
                st.image(image_link, width=150, caption=f"[PIM]({url})")
            except Exception:
                image_link = "https://pimimages.azureedge.net/images/default_recipe.png"
                st.image(image_link, width=150, caption=f"[PIM]({url})")

        with col2:
            st.markdown(f"**Recipe ID:** {row['recipe_id']}")
            st.markdown(
                f"**Main ingredient:** {row['recipe_main_ingredient_name_local']}"
            )
            st.markdown(f"**Cooking time:** {row['cooking_time']} min")

        with col3:
            taxonomy_tags = format_as_tags(
                [
                    tag
                    for tag in (
                        row["taxonomy_name_local"]
                        if isinstance(row["taxonomy_name_local"], list)
                        else []
                    )
                    if not pd.isna(tag)
                ],
                "#E56962",
            )
            st.markdown(f"**Taxonomies:** {taxonomy_tags}", unsafe_allow_html=True)
            st.markdown(f"**Rating:** {int(row['average_rating'])}")
            st.markdown(f"**In universe:** {row['is_in_recipe_universe']}")
        st.markdown("---")

elif column_or_list == "Split View":
    for i in range(0, len(recipe_data), 2):
        row = recipe_data.iloc[i]
        url = row["pim_link"]

        col1, col2 = st.columns(2)

        with col1:
            meta_col1, meta_col2 = st.columns([1, 2])

            with meta_col1:
                try:
                    image_link = str(row["image_link"])
                    st.image(image_link, width=150, caption=f"[PIM]({url})")
                except Exception:
                    image_link = (
                        "https://pimimages.azureedge.net/images/default_recipe.png"
                    )
                    st.image(image_link, width=150, caption=f"[PIM]({url})")

            with meta_col2:
                st.markdown(f"**Recipe ID:** {row['recipe_id']}")
                st.markdown(
                    f"**Main ingredient:** {row['recipe_main_ingredient_name_local']}"
                )
                st.markdown(f"**Cooking time:** {row['cooking_time']} min")
                taxonomy_tags = format_as_tags(
                    [
                        tag
                        for tag in (
                            row["taxonomy_name_local"]
                            if isinstance(row["taxonomy_name_local"], list)
                            else []
                        )
                        if not pd.isna(tag)
                    ],
                    "#E56962",
                )
                st.markdown(f"**Taxonomies:** {taxonomy_tags}", unsafe_allow_html=True)
                st.markdown(f"**Rating:** {int(row['average_rating'])}")
                st.markdown(f"**In universe:** {row['is_in_recipe_universe']}")

        if i + 1 < len(recipe_data):
            next_row = recipe_data.iloc[i + 1]
            next_url = next_row["pim_link"]

            with col2:
                meta_col1, meta_col2 = st.columns([1, 2])

                with meta_col1:
                    try:
                        image_link = str(next_row["image_link"])
                        st.image(
                            image_link,
                            width=150,
                            caption=f"[PIM]({url})",
                        )
                    except Exception:
                        image_link = (
                            "https://pimimages.azureedge.net/images/default_recipe.png"
                        )
                        st.image(
                            image_link,
                            width=150,
                            caption=f"[PIM]({url})",
                        )

                with meta_col2:
                    st.markdown(f"**Recipe ID:** {next_row['recipe_id']}")
                    st.markdown(
                        f"**Main ingredient:** {next_row['recipe_main_ingredient_name_local']}"
                    )
                    st.markdown(f"**Cooking time:** {next_row['cooking_time']} min")
                    taxonomy_tags = format_as_tags(
                        [
                            tag
                            for tag in (
                                next_row["taxonomy_name_local"]
                                if isinstance(next_row["taxonomy_name_local"], list)
                                else []
                            )
                            if not pd.isna(tag)
                        ],
                        "#E56962",
                    )
                    st.markdown(
                        f"**Taxonomies:** {taxonomy_tags}", unsafe_allow_html=True
                    )
                    st.markdown(f"**Rating:** {int(next_row['average_rating'])}")
                    st.markdown(f"**In universe:** {next_row['is_in_recipe_universe']}")
        st.markdown("---")
