import logging

import polars as pl
from aligned import ContractStore
from data_contracts.orders import WeeksSinceRecipe

from preselector.schemas.batch_request import GenerateMealkitRequest

logger = logging.getLogger(__name__)


async def add_ordered_since_feature(
    customer: GenerateMealkitRequest,
    store: ContractStore,
    recipe_features: pl.DataFrame,
    year_week: int,
    selected_recipes: dict[int, int],
) -> pl.DataFrame:
    """
    Adds the recipe quarantining data.

    This means both historical orders, but also expected recipes in the customers selection.

    Args:
        customer (GenerateMealkitRequest): The customers generation request
        store (ContractStore): The store containing all the data sources
        recipe_features (pl.DataFrame): A dataframe containing all recipes candidates
        year_week (int): The year week to load the quarantining data based on. 1 week into the future?
        selected_recipes (dict[int, int]): A realtime input which defines when a recipes was selected

    Returns:
        pl.DataFrame: The recipe_features data frame with a new column containing the quarantining data for each recipe.
    """

    selected_recipe_computation: pl.DataFrame | None = None

    if selected_recipes:
        manual_data = {
            "main_recipe_id": list(selected_recipes.keys()),
            "last_order_year_week": list(selected_recipes.values()),
            "company_id": [customer.company_id] * len(selected_recipes),
            "agreement_id": [customer.agreement_id] * len(selected_recipes),
            "from_year_week": [year_week] * len(selected_recipes),
        }
        selected_recipe_computation = (
            await store.feature_view(WeeksSinceRecipe).process_input(manual_data).to_polars()
        ).cast({"main_recipe_id": pl.Int32})

    return_columns = ["main_recipe_id", "ordered_weeks_ago"]
    default_value = 0

    if selected_recipe_computation is None:
        return recipe_features.with_columns(ordered_weeks_ago=pl.lit(default_value))
    else:
        return (
            recipe_features.cast({"main_recipe_id": pl.Int32})
            .join(selected_recipe_computation.select(return_columns), on="main_recipe_id", how="left")
            .with_columns(pl.col("ordered_weeks_ago").fill_null(default_value))
        )
