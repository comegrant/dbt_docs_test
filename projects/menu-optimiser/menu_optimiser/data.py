import pandas as pd
from data_contracts.sources import azure_dl_creds
from data_contracts.helper import camel_to_snake

RECIPE_BANK_DIRECTORY = "data-science/test-folder/MP20"


async def get_df_from_datalake(directory: str, parquet_filename: str) -> pd.DataFrame:
    df_recipe_bank = (
        await azure_dl_creds.directory(directory)
        .parquet_at(parquet_filename)
        .to_pandas()
    )

    return df_recipe_bank


# TODO: AGATHE: validate
async def get_recipe_bank_data(company_code: str, recipe_bank_env: str) -> pd.DataFrame:
    """Get recipe bank data from datalake."""
    parquet_filename = f"PIM_RecipeBank_{company_code}_{recipe_bank_env}"
    df_recipe_bank = await get_df_from_datalake(RECIPE_BANK_DIRECTORY, parquet_filename)

    return df_recipe_bank


def preprocess_recipe_data(
    df: pd.DataFrame,
    only_universe: bool = True,
) -> pd.DataFrame:
    """Preprocess recipe bank data."""
    # switch from camel case to snake case
    df.columns = pd.Index([camel_to_snake(x) for x in df.columns.to_list()])

    # TODO: AGATHE: switch out with config later
    df = pd.DataFrame(
        df[
            [
                "recipe_id",
                "main_ingredient_id",
                "taxonomy_id",
                "average_rating",
                "price_category",
                "is_universe",
                "cooking_time_from",
                "cooking_time_to",
            ]
        ]
    )

    if only_universe:
        df = pd.DataFrame(df[df["is_universe"]])

    df = pd.DataFrame(df[df["main_ingredient_id"].notna()])
    df["main_ingredient_id"] = df["main_ingredient_id"].astype(int)
    df = df.rename(columns={"price_category": "price_category_id"})
    df["cooking_time"] = (
        df["cooking_time_from"].astype(str) + "_" + df["cooking_time_to"].astype(str)
    )

    # TODO: df_validate

    df = (
        df.groupby("recipe_id")
        .agg(
            taxonomies=("taxonomy_id", list),
            main_ingredient_id=("main_ingredient_id", "first"),
            price_category_id=("price_category_id", "first"),
            average_rating=("average_rating", "first"),
            is_universe=("is_universe", "first"),
            cooking_time_from=("cooking_time_from", "first"),
            cooking_time_to=("cooking_time_to", "first"),
            cooking_time=("cooking_time", "first"),
        )
        .reset_index()
    )

    return df
