import streamlit as st
from streamlit_app.helpers.utils import apply_filters, format_as_tags


st.set_page_config(layout="wide")


def visualize_menu(df_menu):
    for row in df_menu.itertuples():
        st.markdown(f"#### {row.recipe_name.capitalize()}")

        col1, col2, col3, col4 = st.columns([1, 1, 2, 2])

        with col1:
            url = str(row.pim_link)
            image_link = str(row.image_link)
            try:
                st.image(image_link, width=150, caption=f"[PIM link]({url})")
            except Exception:
                image_link = "https://pimimages.azureedge.net/images/default_recipe.png"
                st.image(image_link, width=150, caption=f"[PIM link]({url})")

        with col2:
            st.markdown(f"**Recipe ID:** {row.recipe_id}")
            st.markdown(f"**Main ingredient:** {row.recipe_main_ingredient_name_local}")
            st.markdown(f"**Price category:** {row.price_category_id}")
            st.markdown(f"**Average rating:** {row.average_rating}")
            cooking_time = str(row.cooking_time.replace("_", "-")) + " min"
            st.markdown(f"**Cooking time:** {cooking_time}")

        with col3:
            taxonmy_tags = format_as_tags(row.taxonomies, "#0A1840")
            st.markdown(f"**Taxonomies:** {taxonmy_tags}", unsafe_allow_html=True)

            main_prot_tags = format_as_tags(row.main_protein, "#0A1840", format=True)
            st.markdown(f"**Main protein:** {main_prot_tags}", unsafe_allow_html=True)

            main_carb_tags = format_as_tags(row.main_carb, "#0A1840", format=True)
            st.markdown(f"**Main carb:** {main_carb_tags}", unsafe_allow_html=True)

            cuisine_tags = format_as_tags(row.cuisine, "#0A1840", format=True)
            st.markdown(f"**Cuisine:** {cuisine_tags}", unsafe_allow_html=True)

            dish_type_tags = format_as_tags(row.dish_type, "#0A1840", format=True)
            st.markdown(f"**Dish type:** {dish_type_tags}", unsafe_allow_html=True)

        with col4:
            ingredient_tags = format_as_tags(row.ingredient_name, "#0A1840")
            st.markdown("**Ingredients:**")
            st.markdown(f"{ingredient_tags}", unsafe_allow_html=True)


def main(df_new, df_old, df_new_ingredients, df_old_ingredients):
    selection = st.segmented_control(
        "Menu",
        options=["New Menu", "Old Menu"],
        selection_mode="single",
        default="New Menu",
    )

    if selection == "New Menu":
        df_menu = df_new
        df_ingredients = df_new_ingredients
    elif selection == "Old Menu":
        df_menu = df_old
        df_ingredients = df_old_ingredients

    num_ingredients = len(df_ingredients)
    num_cold_storage = df_ingredients["is_cold_storage"].sum()
    num_non_cold_storage = num_ingredients - num_cold_storage

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Number of ingredients", value=num_ingredients)
    with col2:
        st.metric(label="Number of cold storage ingredients", value=num_cold_storage)
    with col3:
        st.metric(
            label="Number of non-cold storage ingredients", value=num_non_cold_storage
        )

    st.write("---")

    visualize_menu(df_menu)


if __name__ == "__main__":
    company = st.session_state["company"]
    df_old = st.session_state["old_menu_data"]
    df_new = st.session_state["new_menu_data"]
    df_new_ingredients = st.session_state["new_menu_ingredients"]
    df_old_ingredients = st.session_state["old_menu_ingredients"]
    payload = st.session_state["payload"]

    st.title(f"Recipes for {company}")

    df_new, df_old, _ = apply_filters(df_new, df_old)

    main(df_new, df_old, df_new_ingredients, df_old_ingredients)
