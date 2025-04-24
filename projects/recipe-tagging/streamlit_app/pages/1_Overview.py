import streamlit as st

st.set_page_config(layout="wide")

data = st.session_state.data


taxonomy_types = {
    "Preference": "Select Preferences",
    "Protein Category": "Select Categories",
    "Protein": "Select Proteins",
    "Dish": "Select Dishes",
    "Cuisine": "Select Cuisines",
    "Trait": "Select Traits",
}

selected_filters = {}

for taxonomy, label in taxonomy_types.items():
    values = data[data["taxonomy_type_name"] == taxonomy]["taxonomy_name_local"].unique()
    selected = st.sidebar.multiselect(label, values)
    if selected:
        selected_filters[taxonomy] = selected

matching_recipes = None

for taxonomy, selected_values in selected_filters.items():
    filtered_data = data[data["taxonomy_type_name"] == taxonomy]

    recipe_counts = (
        filtered_data[filtered_data["taxonomy_name_local"].isin(selected_values)]
        .groupby("recipe_id")["taxonomy_name_local"]
        .nunique()
    )

    recipe_ids = set(recipe_counts[recipe_counts == len(selected_values)].index)

    if matching_recipes is None:
        matching_recipes = recipe_ids
    else:
        matching_recipes.intersection_update(recipe_ids)


if matching_recipes is not None:
    data_filtered = data[data["recipe_id"].isin(matching_recipes)]
else:
    data_filtered = data


st.title("Recipe Display")

col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    select_num_columns = st.segmented_control(
        "Number of Columns",
        options=[1, 2, 3, 4],
        default=3,
    )
with col2:
    show_all_recipes = st.segmented_control(
        "Show all recipes",
        options=["True", "False"],
        default="False",
    )

with col3:
    st.write("")

st.write(f"Displaying {data_filtered['recipe_id'].nunique()} recipes")
st.markdown("---")

grouped_df = (
    data_filtered.groupby(["recipe_id", "recipe_name", "image_url"])
    .apply(lambda x: x.groupby("taxonomy_type_name")["taxonomy_name_local"].apply(list).to_dict())
    .reset_index(name="tags")
)

if show_all_recipes == "False":
    grouped_df = grouped_df.head(20)

columns = st.columns(select_num_columns)

for idx, row in grouped_df.iterrows():
    col = columns[idx % select_num_columns]

    with col:
        recipe_name = row["recipe_name"].capitalize().rstrip()
        st.markdown(f"**{recipe_name}**")

        img_col, text_col = st.columns([1, 2])

        with img_col:
            st.image(row["image_url"], use_container_width=True)

        with text_col:
            for category, tag_list in row["tags"].items():
                formatted_tags = ", ".join(tag_list)
                st.markdown(f"**{category}:** {formatted_tags}")

    if (idx + 1) % select_num_columns == 0:
        st.markdown("---")
        columns = st.columns(select_num_columns)
