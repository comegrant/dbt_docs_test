"""
Preprocessing functions for the menu optimiser.
"""

import pandas as pd
from menu_optimiser.config import (
    cuisine_taxonomies,
    dish_taxonomies,
    protein_filet,
    protein_ground_meat,
    protein_mixed_meat,
)
from data_contracts.helper import camel_to_snake
from menu_optimiser.config import RecipeConfig

recipe_config = RecipeConfig()


# ----------------------------
# GENERAL
# ----------------------------
def convert_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame column names from camelCase to snake_case format."""
    df.columns = [camel_to_snake(col) for col in df.columns]
    return df


def normalize_column_text(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Convert the entries in the column to lowercase and swap any space with a _"""
    df[column] = df[column].str.lower().str.replace(" ", "_")
    return df


def fill_empty_or_nan_list_column(
    df: pd.DataFrame,
    column: str,
    fill_value: list[int | str],  # TODO: should be any?
) -> pd.DataFrame:
    """Fill empty lists or NaN values in a DataFrame column with a specified fill value."""
    mask_empty = df[column].apply(lambda x: isinstance(x, list) and len(x) == 0)
    mask_nan = df[column].isna()
    df.loc[mask_empty | mask_nan, column] = df.loc[mask_empty | mask_nan, column].apply(
        lambda x: fill_value
    )
    return df


# ----------------------------
# RECIPE BANK
# ----------------------------
def select_and_clean_columns(
    df: pd.DataFrame, only_universe: bool = True
) -> pd.DataFrame:
    """Select and clean the columns from the recipe bank data."""
    df = pd.DataFrame(df[recipe_config.recipe_bank_columns].copy())

    if only_universe:
        df = pd.DataFrame(df[df["is_universe"]])

    df = pd.DataFrame(df[df["main_ingredient_id"].notna()])
    df["main_ingredient_id"] = df["main_ingredient_id"].astype(int)
    df["cooking_time"] = (
        df["cooking_time_from"].astype(str) + "_" + df["cooking_time_to"].astype(str)
    )

    return df


def aggregate_recipe_data(data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the recipe data."""
    df = data.copy()
    aggregation_dict = {
        key: (name, agg_func)
        for key, (name, agg_func) in recipe_config.aggregation_config.items()
    }

    grouped_df = pd.DataFrame(
        df.groupby("recipe_id", as_index=False).agg(**aggregation_dict)
    )
    return grouped_df


def preprocess_recipe_bank_data(
    df: pd.DataFrame, only_universe: bool = True
) -> pd.DataFrame:
    """Preprocess the recipe bank data."""
    df = convert_column_names(df)
    df = select_and_clean_columns(df, only_universe)
    df = aggregate_recipe_data(df)
    return df


# ----------------------------
# RECIPE TAGGING
# ----------------------------
def preprocess_recipe_tagging(df_recipe_tagging: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the recipe tagging data.
    """
    df_recipe_tagging["extra_taxonomies"] = df_recipe_tagging["extra_taxonomies"].apply(
        lambda x: [int(tax) for tax in x]
    )
    df_recipe_tagging["cuisine"] = df_recipe_tagging["extra_taxonomies"].apply(
        lambda x: [tax for tax in x if tax in cuisine_taxonomies]
    )

    df_recipe_tagging["dish_type"] = df_recipe_tagging["extra_taxonomies"].apply(
        lambda x: [tax for tax in x if tax in dish_taxonomies]
    )

    # for protein, check if any of the extra_taxonomies are in the protein_filet, protein_ground_meat,
    # or protein_mixed_meat lists, and if so, add either 'filet', 'ground_meat', or 'mixed_meat' to the protein column
    df_recipe_tagging["protein_processing"] = df_recipe_tagging[
        "extra_taxonomies"
    ].apply(
        lambda x: "filet"
        if any(tax in protein_filet for tax in x)
        else "ground_meat"
        if any(tax in protein_ground_meat for tax in x)
        else "mixed_meat"
        if any(tax in protein_mixed_meat for tax in x)
        else "unknown"
    )

    # drop extra_taxonomies
    df_recipe_tagging = df_recipe_tagging.drop(columns=["extra_taxonomies"])

    return df_recipe_tagging


# ----------------------------
# RECIPE CARBS
# ----------------------------
def preprocess_carbs(df_carbs: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the carbs data.
    """
    for column in recipe_config.ingredient_columns_to_normalize:
        df_carbs = normalize_column_text(df_carbs, column)

    df_carbs = pd.DataFrame(
        df_carbs[~df_carbs["main_group"].isin(recipe_config.carb_remove_groups)]
    )

    df_carbs["main_carb"] = df_carbs.apply(
        lambda row: row["category_group"]
        if row["category_group"] in recipe_config.carb_category_groups
        else row["product_group"],
        axis=1,
    )

    df_carbs = (
        df_carbs.groupby("recipe_id")["main_carb"]
        .apply(list)
        .to_frame("main_carb")
        .reset_index()
    )

    return df_carbs


# ----------------------------
# RECIPE PROTEIN
# ----------------------------
def preprocess_protein(df_protein: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the protein data.
    """
    for column in recipe_config.ingredient_columns_to_normalize:
        df_protein = normalize_column_text(df_protein, column)

    df_protein = pd.DataFrame(
        df_protein[~df_protein["main_group"].isin(recipe_config.protein_remove_groups)]
    )

    df_protein["main_protein"] = df_protein.apply(
        lambda row: row["main_group"]
        if row["main_group"] in recipe_config.protein_main_groups
        else row["category_group"],
        axis=1,
    )

    df_protein["main_protein"] = df_protein.apply(
        lambda row: row["product_group"]
        if row["category_group"] in recipe_config.protein_product_groups
        else row["main_protein"],
        axis=1,
    )
    df_protein = df_protein.drop_duplicates(
        subset=["recipe_id", "main_protein"], keep="first"
    )

    df_protein = (
        df_protein.groupby("recipe_id")["main_protein"]
        .apply(list)
        .to_frame("main_protein")
        .reset_index()
    )

    return df_protein


# ----------------------------
# FINAL DATASET
# ----------------------------
def preprocess_final_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the final dataset.
    """
    df = df.drop_duplicates(subset=["recipe_id"])

    for col, fill_val in recipe_config.fill_columns.items():
        df = fill_empty_or_nan_list_column(df, col, fill_val)

    df["protein_processing"] = df["protein_processing"].fillna("unknown")

    return df
