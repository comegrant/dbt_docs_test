import streamlit as st
from streamlit_app.helpers.data import filter_data_overview
from streamlit_app.helpers.utils import get_weeks, user_friendly_score

st.set_page_config(page_title="Attribute Scoring", page_icon=":tomato:")
st.title("Overview")


if not st.session_state["authenticated"]:
    st.error("Please authenticate to use the app.")
    st.stop()

recipe_data = st.session_state["recipe_data"]
week_list = get_weeks()

select_company = st.sidebar.selectbox("Which companies do you want to look at?", ["AMK", "GL", "LMK", "RT"])
select_year_week = st.sidebar.multiselect("Which week do you want to look at?", week_list, default="2024-45")
selected_year_week_tuples = [(int(year), int(week)) for year, week in (yw.split("-") for yw in select_year_week)]

filtered_data = filter_data_overview(df=recipe_data, company=select_company, year_week=selected_year_week_tuples)

if len(filtered_data) == 0:
    st.error("""
            🔍 No predictions available for your selection.
            Try choosing a different week or company to see results.
            """)

col_sort1, col_sort2 = st.columns(2)

with col_sort1:
    sort_by = st.segmented_control(
        "Sort by",
        options=["Chef's Favorite", "Family Friendly"],
        default="Family Friendly",
        selection_mode="single",
    )

with col_sort2:
    sort_direction = st.segmented_control(
        "Direction",
        options=["Low to High", "High to Low"],
        default="Low to High",
        selection_mode="single",
    )

if sort_by:
    sort_column = "chefs_favorite_probability" if sort_by == "Chef's Favorite" else "family_friendly_probability"
    ascending = sort_direction == "Low to High"
    filtered_data = filtered_data.sort_values(by=[sort_column], ascending=ascending)

with st.expander("Get more information about the score"):
    st.write("""
                For each critieria, e.g. family friendliness, the score level means:

            - **Very High**: This is an excellent score, indicating a strong alignment with our desired criteria.
            - **High**: This is a good score, showing a high level of alignment with the criteria.
            - **Medium**: This score is moderate, suggesting an average alignment with the criteria.
            - **Low**: This score indicates limited alignment with the criteria.
            - **Very Low**: This is a low score, indicating minimal alignment with the criteria.
        """)

for _, row in filtered_data.iterrows():
    st.write(f"#### {row['recipe_name'].capitalize()}")
    url = row["pim_link"]
    chef_score = user_friendly_score(row["chefs_favorite_probability"])
    family_score = user_friendly_score(row["family_friendly_probability"])

    col1, col2 = st.columns([1, 2])

    with col1:
        st.image(row["recipe_image_link"], width=200, caption=f"Recipe ID: {row['recipe_id']}")

    with col2:
        st.markdown(f"**Recipe ID:** {row['recipe_id']}")
        st.markdown(f"**Main ingredient:** {row['recipe_main_ingredient_name']}")
        st.markdown(f"**Recipe difficulty:** {row['recipe_difficulty_name'].capitalize()}")
        st.markdown(f"**Average cooking time:** {(row['cooking_time_from']+row['cooking_time_to'])/2} min")
        st.markdown(f"**Menu week:** {row['menu_week']}")
        st.markdown(f"**Chef's Favoriteness:** {chef_score}")
        st.markdown(f"**Family Friendliness:** {family_score}")
        st.markdown(f"**PIM:** [link](%s)" % url)

    st.markdown("---")
