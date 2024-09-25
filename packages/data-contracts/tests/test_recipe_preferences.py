from datetime import datetime, timezone

import polars as pl
import pytest
from aligned import ContractStore
from aligned.schemas.feature_view import RetrivalRequest
from data_contracts.in_mem_source import InMemorySource
from data_contracts.recipe import (
    AllRecipeIngredients,
    IngredientAllergiesPreferences,
    RecipePreferences,
    join_recipe_and_allergies,
)


@pytest.mark.asyncio
async def test_join_allergies_and_recipe_preferences() -> None:
    """
    Tests that we are able to join preferences from different sources.

    This test is designed to test three scenarios
    1. We are able to join preferences from 'recipe preferences' and allergies together
    2. We are able to join preferences that only exist in allergies
    3. We are able to join preferences that only exist in 'recipe preference'
    """

    store = ContractStore.empty()

    store.add_feature_view(IngredientAllergiesPreferences)
    store.add_feature_view(AllRecipeIngredients)
    store.add_feature_view(RecipePreferences)

    now = datetime.now(tz=timezone.utc)

    store = store.update_source_for(
        IngredientAllergiesPreferences.location,
        InMemorySource.from_values({
            "ingredient_id": [1, 2, 3],
            "allergy_id": [1, 2, 3],
            "has_trace_of": [False, False, False],
            "preference_id": ["1", "2", "3"],
            "allergy_name": ["a", "b", "c"],
        })
    ).update_source_for(
        AllRecipeIngredients.location,
        InMemorySource.from_values({
            "recipe_id": [1, 1, 3, 3],
            "ingredient_id": [1, 2, 3, 2],
            "portion_id": [1, 1, 1, 1],
            "created_at": [now, now, now, now],
            "portion_size": [2, 2, 2, 2],
            "ingredient_name": ["", "", "", ""],
            "supplier_name": ["", "", "", ""],
        })
    ).update_source_for(
        RecipePreferences.location,
        InMemorySource.from_values({
            "recipe_id": [1, 2],
            "portion_size": [2, 2],
            "loaded_at": [now, now],
            "portion_id": [1, 1],
            "menu_year": [1, 1],
            "menu_week": [1, 1],
            "main_recipe_id": [1, 2],
            "preference_ids": [["4", "5"], ["4"]],
            "preferences": [[], []],
        })
    )

    dummy_request = RetrivalRequest("", RecipePreferences.location, set(), set(), set())

    recipes = (await join_recipe_and_allergies(dummy_request, store)).collect()

    assert recipes.height == 3
    assert recipes.unique("recipe_id").height == 3
    assert recipes.filter(pl.col("recipe_id") == 1).explode("preference_ids").height == 4
    assert recipes.filter(pl.col("recipe_id") == 2).explode("preference_ids").height == 1
    assert recipes.filter(pl.col("recipe_id") == 3).explode("preference_ids").height == 2
