import asyncio
from contextlib import suppress
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field
from pydantic_argparser import parse_args

from catalog_connector import connection
from data_contracts.sources import data_science_data_lake


class Args(BaseModel):
    environment: Literal["prod", "test", "dev"] = Field("dev")


# TODO:make it a dbt model
def get_carbs_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        dr.recipe_name,
        gi.main_group_name as main_group,
        gi.category_group_name as category_group,
        gi.product_group_name as product_group
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and intt.is_main_carbohydrate
    """).toPandas()
    return df


# TODO:make it a dbt model
def get_protein_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        dr.recipe_name,
        gi.main_group_name as main_group,
        gi.category_group_name as category_group,
        gi.product_group_name as product_group
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and intt.is_main_protein
    """).toPandas()
    return df


def get_ingredients_data() -> pd.DataFrame:
    df = connection.sql("""
    select distinct
        dr.recipe_id,
        gi.ingredient_id,
        gi.ingredient_name,
        gi.is_cold_storage
    from gold.dim_recipes dr
    left join intermediate.int_recipe_ingredients_joined intt
        on dr.recipe_id = intt.recipe_id
    left join gold.dim_portions dp
        on intt.portion_id = dp.portion_id
    left join gold.dim_ingredients gi
        on intt.ingredient_id = gi.ingredient_id
    where
        dr.is_in_recipe_universe
        and dp.portion_name_local in ('4', '1')
        and gi.ingredient_id <> 4062 --generic basis
        and gi.main_group_name <> 'Basis'
    """).toPandas()

    return df


async def write_to_data_lake(df: pd.DataFrame, filename: str, env: str):
    """Write dataframe to azure data lake."""
    df_clean = df.copy()
    df_clean.attrs.clear()

    await data_science_data_lake.config.storage.write(
        path=f"data-science/menu_generator/{env}/{filename}",
        content=df_clean.to_parquet(index=False),
    )


async def main(args: Args) -> None:
    df_carbs = get_carbs_data()
    df_protein = get_protein_data()
    df_ingredients = get_ingredients_data()
    await write_to_data_lake(
        df=df_carbs,
        filename="recipe_carbohydrates.parquet",
        env=args.environment,
    )

    await write_to_data_lake(
        df=df_protein,
        filename="recipe_proteins.parquet",
        env=args.environment,
    )

    await write_to_data_lake(
        df=df_ingredients,
        filename="recipe_ingredients.parquet",
        env=args.environment,
    )


if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv())
    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(args=parse_args(Args)))
