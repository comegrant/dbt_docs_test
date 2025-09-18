import pandas as pd
from catalog_connector import connection
from typing import Union
import streamlit as st

from menu_optimiser.db import (
    get_recipe_tagging_data,
    get_recipe_carbs_data,
    get_recipe_protein_data,
)
from menu_optimiser.preprocessing import (
    preprocess_recipe_tagging,
    preprocess_carbs,
    preprocess_protein,
)

URLS = {
    "img_url": "https://pimimages.azureedge.net/images/resized/",
    "pim_url": "https://pim.cheffelo.com/recipes/edit/",
}


async def get_new_recipe_data(data: pd.DataFrame) -> pd.DataFrame:
    recipes = data["recipe_id"].tolist()

    df = connection.sql(
        """
        select
            dr.recipe_id,
            dr.recipe_name,
            dr.recipe_main_ingredient_name_local,
            rm.recipe_photo
        from gold.dim_recipes dr
        left join silver.pim__recipe_metadata rm
            on dr.recipe_metadata_id = rm.recipe_metadata_id
        where recipe_id in ({})
        """.format(",".join(map(str, recipes)))
    ).toPandas()

    df["image_link"] = URLS["img_url"] + df["recipe_photo"].astype(str)
    df["pim_link"] = URLS["pim_url"] + df["recipe_id"].astype(str)

    final_df = pd.merge(data, df, on="recipe_id", how="left")

    return final_df


async def get_extra_columns_for_old_menu(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the extra columns for the old menu.
    """
    df_recipe_tagging = await get_recipe_tagging_data()
    df_carbs = await get_recipe_carbs_data()
    df_protein = await get_recipe_protein_data()

    df_recipe_tagging = preprocess_recipe_tagging(df_recipe_tagging)
    df_carbs = preprocess_carbs(df_carbs)
    df_protein = preprocess_protein(df_protein)

    df_full = df.merge(df_recipe_tagging, on="recipe_id", how="left")
    df_full = df_full.merge(df_carbs, on="recipe_id", how="left")
    df_full = df_full.merge(df_protein, on="recipe_id", how="left")

    return df_full


def map_new_taxonomies(
    data: pd.DataFrame,
    col: str,
    list_of_taxonomies: list[int] | None = None,
    keep_ids: bool = False,
) -> pd.DataFrame:
    """Map list of taxonomy id's to the name. A list of valid taxonomies can be included if wanted"""

    exploded = data.explode(col)
    exploded = exploded.rename(columns={col: "taxonomy_id"})

    if list_of_taxonomies:
        relevant_taxonomies = list_of_taxonomies
    else:
        relevant_taxonomies = exploded["taxonomy_id"].dropna().unique().tolist()
        # if taxonomy = -1, drop it
        # due to data preprocessing, -1 is used to represent unknowns
        relevant_taxonomies = [tax for tax in relevant_taxonomies if tax != -1]

    exploded = exploded[exploded["taxonomy_id"].isin(relevant_taxonomies)]

    taxonomies = connection.sql(
        """
        select
            taxonomy_id,
            taxonomy_name_local
        from gold.dim_taxonomies
        where taxonomy_id in ({})
        """.format(",".join(map(str, relevant_taxonomies)))
    ).toPandas()

    merged = pd.merge(exploded, taxonomies, on="taxonomy_id", how="left")

    taxonomy_names = merged.groupby("recipe_id")["taxonomy_name_local"].agg(list)
    final_df = pd.merge(data, taxonomy_names, on="recipe_id", how="left")

    if keep_ids:
        taxonomy_ids = merged.groupby("recipe_id")["taxonomy_id"].agg(list)
        final_df = pd.merge(
            final_df, taxonomy_ids.rename(f"old_{col}"), on="recipe_id", how="left"
        )

    final_df = final_df.drop(columns=[col])
    # rename column
    final_df.rename(columns={"taxonomy_name_local": col}, inplace=True)

    return final_df


def create_menu_with_ingredients(
    df_menu: pd.DataFrame, raw_recipe_ingredients: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_recipe_ingredients = raw_recipe_ingredients[
        raw_recipe_ingredients["recipe_id"].isin(df_menu["recipe_id"])
    ]

    df_aggregated = (
        df_recipe_ingredients.groupby("recipe_id")
        .agg(ingredient_name=("ingredient_name", list))
        .reset_index()
    )

    df_merged = df_menu.merge(
        df_aggregated,
        on="recipe_id",
        how="left",
    )

    df_ingredients = df_recipe_ingredients.drop(columns=["recipe_id"])
    df_ingredients = df_ingredients.drop_duplicates()

    return df_merged, df_ingredients


def apply_filters(df_new, df_old):
    st.sidebar.header("🔍 Filters")

    filter_configs = [
        {
            "label": "Taxonomies",
            "column": "taxonomies",
            "options": sorted(
                set(
                    t
                    for sublist in df_new["taxonomies"].dropna()
                    for t in (sublist if isinstance(sublist, list) else [])
                )
            ),
            "is_list": True,
        },
        {
            "label": "Main Ingredients",
            "column": "recipe_main_ingredient_name_local",
            "options": sorted(
                df_new["recipe_main_ingredient_name_local"].dropna().unique()
            ),
            "is_list": False,
        },
        {
            "label": "Price Categories",
            "column": "price_category_id",
            "options": sorted(df_new["price_category_id"].dropna().unique()),
            "is_list": False,
        },
        {
            "label": "Cooking Time To",
            "column": "cooking_time_to",
            "options": sorted(df_new["cooking_time_to"].dropna().unique()),
            "is_list": False,
        },
        {
            "label": "Main Proteins",
            "column": "main_protein",
            "options": sorted(
                set(
                    t
                    for sublist in df_new["main_protein"].dropna()
                    for t in (sublist if isinstance(sublist, list) else [])
                )
            ),
            "is_list": True,
        },
        {
            "label": "Main Carbohydrates",
            "column": "main_carb",
            "options": sorted(
                set(
                    t
                    for sublist in df_new["main_carb"].dropna()
                    for t in (sublist if isinstance(sublist, list) else [])
                )
            ),
            "is_list": True,
        },
        {
            "label": "Cuisines",
            "column": "cuisine",
            "options": sorted(
                set(
                    t
                    for sublist in df_new["cuisine"].dropna()
                    for t in (sublist if isinstance(sublist, list) else [])
                )
            ),
            "is_list": True,
        },
        {
            "label": "Dish Types",
            "column": "dish_type",
            "options": sorted(
                set(
                    t
                    for sublist in df_new["dish_type"].dropna()
                    for t in (sublist if isinstance(sublist, list) else [])
                )
            ),
            "is_list": True,
        },
    ]

    selected_filters = {}

    for config in filter_configs:
        selected = st.sidebar.multiselect(config["label"], config["options"])
        selected_filters[config["column"]] = selected

        if selected:
            if config["is_list"]:

                def filter_func(x):
                    if isinstance(x, list):
                        return any(t in x for t in selected)
                    else:
                        return False
            else:

                def filter_func(x):
                    return x in selected

            df_new = df_new[df_new[config["column"]].apply(filter_func)]
            df_old = df_old[df_old[config["column"]].apply(filter_func)]

    return df_new, df_old, selected_filters


def format_as_tags(
    preferences: Union[str, list[str], None], color: str, format: bool = False
) -> str:
    if preferences is None or (isinstance(preferences, float) and pd.isna(preferences)):
        preferences = ["Unknown"]

    pref_list: list[str] = (
        [preferences] if isinstance(preferences, str) else list(preferences)
    )

    if format:
        pref_list = [pref.replace("_", " ").capitalize() for pref in pref_list]

    tags: str = " ".join(
        [
            f"<span style='background-color:{color}; "
            f"color:white; padding:2px 4px; margin:1px; "
            f"border-radius:4px; font-size:0.9em;'>"
            f"{pref}</span>"
            for pref in pref_list
        ]
    )
    return tags
