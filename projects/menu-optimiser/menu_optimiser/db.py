"""
Data loading and validation from azure datalake.
"""

import json
import pandas as pd
import pandera.pandas as pa
from data_contracts.sources import azure_dl_creds
from menu_optimiser.config import PathConfig

path = PathConfig()


# ----------------------------
# RAW DATA
# ----------------------------
async def get_df_from_datalake(directory: str, parquet_filename: str) -> pd.DataFrame:
    """
    Load a Parquet file from Azure Data Lake into a pandas DataFrame.

    Args:
        directory (str): Directory path in the data lake.
        parquet_filename (str): Parquet file name.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    df = (
        await azure_dl_creds.directory(directory)
        .parquet_at(parquet_filename)
        .to_pandas()
    )
    return df


async def get_json_from_datalake(directory: str, json_filename: str) -> dict:
    """
    Load a JSON file from Azure Data Lake and parse it into a Python dictionary.

    Args:
        directory (str): Directory path in the data lake.
        json_filename (str): JSON file name.

    Returns:
        dict: Parsed JSON content.
    """
    json_blob = azure_dl_creds.directory(directory).json_at(json_filename)
    json_bytes = await json_blob.read()
    return json.loads(json_bytes.decode("utf-8"))


# ----------------------------
# SCHEMAS
# ----------------------------
def get_recipe_bank_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "RecipeId": pa.Column(int, nullable=False),
            "MainIngredientId": pa.Column(float, nullable=True),
            "AverageRating": pa.Column(float, nullable=True),
            "TaxonomyId": pa.Column(int, nullable=False),
            "IsUniverse": pa.Column(bool, nullable=False),
            "CookingTimeFrom": pa.Column(int, nullable=False),
            "CookingTimeTo": pa.Column(int, nullable=False),
            "PriceCategory": pa.Column(int, nullable=False),
        },
        strict=False,
        coerce=True,
    )


def get_recipe_main_ingredient_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "recipe_id": pa.Column(int, nullable=False),
            "recipe_name": pa.Column(str, nullable=True),
            "main_group": pa.Column(str, nullable=True),
            "category_group": pa.Column(str, nullable=True),
            "product_group": pa.Column(str, nullable=True),
        },
        strict=False,
        coerce=True,
    )


def get_recipe_ingredients_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "recipe_id": pa.Column(int, nullable=False),
            "ingredient_id": pa.Column(int, nullable=False),
            "is_cold_storage": pa.Column(bool, nullable=False),
        },
        strict=False,
        coerce=True,
    )


def get_recipe_tagging_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "recipe_id": pa.Column(int, nullable=False),
            "extra_taxonomies": pa.Column(list[str], nullable=True),
        },
        strict=False,
        coerce=True,
    )


# ----------------------------
# DATA
# ----------------------------
async def get_recipe_bank_data(company_code: str, recipe_bank_env: str) -> pd.DataFrame:
    """Get the recipe bank data from the datalake."""

    filename = f"PIM_RecipeBank_{company_code}_{recipe_bank_env}"
    df = await get_df_from_datalake(
        directory=path.recipe_bank_directory,
        parquet_filename=filename,
    )
    return get_recipe_bank_schema().validate(df)


async def get_recipe_carbs_data() -> pd.DataFrame:
    """Get the recipe carbs data from the datalake."""
    df = await get_df_from_datalake(
        directory=path.recipe_main_ingredients_directory,
        parquet_filename="recipe_carbohydrates.parquet",
    )
    return get_recipe_main_ingredient_schema().validate(df)


async def get_recipe_protein_data() -> pd.DataFrame:
    """Get the recipe protein data from the datalake."""
    df = await get_df_from_datalake(
        directory=path.recipe_main_ingredients_directory,
        parquet_filename="recipe_proteins.parquet",
    )
    return get_recipe_main_ingredient_schema().validate(df)


async def get_recipe_ingredients_data() -> pd.DataFrame:
    """Get the recipe ingredients data from the datalake."""
    df = await get_df_from_datalake(
        directory=path.recipe_main_ingredients_directory,
        parquet_filename="recipe_ingredients.parquet",
    )
    return get_recipe_ingredients_schema().validate(df)


async def get_recipe_tagging_data() -> pd.DataFrame:
    """Get the recipe tagging data from the datalake."""
    json_data = await get_json_from_datalake(
        directory=path.recipe_tagging_directory,
        json_filename="20250618_102534_output.json",  # TODO: make this dynamic
    )
    df = pd.DataFrame(
        [
            {
                "recipe_id": recipe["recipe_id"],
                "extra_taxonomies": list(recipe["taxonomy_ids_dk"].keys())
                if "taxonomy_ids_dk" in recipe
                else list(recipe["taxonomy_ids_no"].keys())
                if "taxonomy_ids_no" in recipe
                else list(recipe["taxonomy_ids_se"].keys())
                if "taxonomy_ids_se" in recipe
                else [],
            }
            for recipe in json_data["recipes"]
        ]
    )

    return get_recipe_tagging_schema().validate(df)
