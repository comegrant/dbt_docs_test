import streamlit as st
from constants.companies import get_company_by_id
from streamlit_app.helpers.utils import plot_donut

st.set_page_config(layout="wide")
st.title("Target Overview")


response = st.session_state["response"]
company = st.session_state["company"]
company_name = get_company_by_id(company.company_id.upper()).company_name
week = st.session_state["week"]
year = st.session_state["year"]
status_msg = st.session_state["status_msg"]

recipe_data = st.session_state["recipe_data"]
recipes = st.session_state["recipes"]

st.subheader(f"{company_name}")
st.write(f"**Week {week}, {year}**")

# TODO: clean up the filtering
# FILTERS
st.sidebar.header("🔍 Filters")
taxonomy_options = sorted(
    set(
        t
        for sublist in recipe_data["taxonomy_name_local"].dropna()
        for t in (sublist if isinstance(sublist, list) else [])
    )
)
main_ingredient_options = sorted(
    recipe_data["recipe_main_ingredient_name_local"].dropna().unique()
)
price_category_options = sorted(recipe_data["price_category_id"].dropna().unique())
cooking_time_options = sorted(recipe_data["cooking_time"].dropna().unique())

# Sidebar multiselect filters
selected_taxonomies = st.sidebar.multiselect("Taxonomies", taxonomy_options)
selected_main_ingredients = st.sidebar.multiselect(
    "Main Ingredients", main_ingredient_options
)
selected_price_categories = st.sidebar.multiselect(
    "Price Categories", price_category_options
)
selected_cooking_times = st.sidebar.multiselect("Cooking Times", cooking_time_options)


filtered_data = recipe_data.copy()

if selected_taxonomies:
    filtered_data = filtered_data[
        filtered_data["taxonomy_name_local"].apply(
            lambda x: any(t in x for t in selected_taxonomies)
            if isinstance(x, list)
            else False
        )
    ]


if selected_main_ingredients:
    filtered_data = filtered_data[
        filtered_data["recipe_main_ingredient_name_local"].isin(
            selected_main_ingredients
        )
    ]


if selected_price_categories:
    filtered_data = filtered_data[
        filtered_data["price_category_id"].isin(selected_price_categories)
    ]

if selected_cooking_times:
    filtered_data = filtered_data[
        filtered_data["cooking_time"].isin(selected_cooking_times)
    ]


# KPI'S
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(
        label="Total Recipes",
        value=len(filtered_data),
    )
with col2:
    st.metric(
        label="Recipes in Universe",
        value=filtered_data["is_in_recipe_universe"].sum(),
    )
with col3:
    st.metric(
        label="Average Rating",
        value=round(filtered_data["average_rating"].mean(), 1),
    )

with st.expander("View status message"):
    st.write(status_msg)

st.divider()

# TAXONOMY OVERVIEW
st.subheader("Target details")

taxonomy_counts = filtered_data["taxonomy_name_local"].explode().value_counts()
taxonomy_percentages = (taxonomy_counts / taxonomy_counts.sum() * 100).round(1)

# taxonomy_df = (
#     pd.DataFrame({"Count": taxonomy_counts, "Percentage (%)": taxonomy_percentages})
#     .reset_index()
#     .rename(columns={"taxonomy_name_local": "Taxonomy"})
# )

# st.dataframe(taxonomy_df)


table_md = "| Taxonomy | Count | Percentage |\n|---|---:|---:|\n"
for taxonomy, count in taxonomy_counts.items():
    percent = taxonomy_percentages[taxonomy]
    table_md += f"| {taxonomy} | {count} | {percent:.1f}% |\n"

st.markdown(table_md)
st.divider()

# DONUT CHARTS
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    st.plotly_chart(
        plot_donut(
            filtered_data["recipe_main_ingredient_name_local"], "Main Ingredients"
        ),
        use_container_width=True,
    )
with col2:
    st.plotly_chart(
        plot_donut(filtered_data["cooking_time"], "Cooking Time"),
        use_container_width=True,
    )
with col3:
    st.plotly_chart(
        plot_donut(filtered_data["price_category_id"], "Price Category"),
        use_container_width=True,
    )


st.divider()
st.subheader("Full Recipe Data")
st.dataframe(recipe_data)
