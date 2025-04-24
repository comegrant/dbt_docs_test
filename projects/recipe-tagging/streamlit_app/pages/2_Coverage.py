import pandas as pd
import streamlit as st

data = st.session_state.data

### COPYPASTE
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
    values = data[data["taxonomy_type_name"] == taxonomy][
        "taxonomy_name_local"
    ].unique()
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
### COPYPASTE END


grouped_df_filtered = (
    data_filtered.groupby(["recipe_id", "recipe_name"])
    .apply(
        lambda x: x.groupby("taxonomy_type_name")["taxonomy_name_local"]
        .apply(list)
        .to_dict()
    )
    .reset_index(name="tags")
)

grouped_df = (
    data.groupby(["recipe_id", "recipe_name"])
    .apply(
        lambda x: x.groupby("taxonomy_type_name")["taxonomy_name_local"]
        .apply(list)
        .to_dict()
    )
    .reset_index(name="tags")
)


def expand_metadata(df: pd.DataFrame, tags: str = "tags") -> pd.DataFrame:
    df = df.copy()
    df = df[df[tags].apply(lambda x: isinstance(x, dict))]

    extracted_data = pd.json_normalize(df[tags])
    df = df.join(extracted_data)
    return df.drop(columns=[tags])


df = expand_metadata(grouped_df_filtered)
df_og = expand_metadata(grouped_df)

st.title("Coverage")
st.markdown("---")

categories = ["Cuisine", "Protein", "Dish", "Preference", "Protein Category", "Trait"]

for category in categories:
    st.subheader(f"{category} Distribution")

    counts_og = df_og[category].explode().value_counts().reset_index()
    counts_og.columns = [category, "count"]
    counts_og["Color"] = "Original"

    counts = df[category].explode().value_counts().reset_index()
    counts.columns = [category, "count"]
    counts["Color"] = "Filtered"

    combined_df = pd.concat([counts_og, counts])

    st.bar_chart(combined_df, x=category, y="count", color="Color", stack=False)

    st.markdown("---")

st.subheader("Full Dataset")
st.dataframe(df_og)
