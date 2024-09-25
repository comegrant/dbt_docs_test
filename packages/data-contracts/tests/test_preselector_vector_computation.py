from datetime import date, datetime, timezone

import polars as pl
import pytest
from aligned import ContractStore
from aligned.request.retrival_request import RetrivalRequest
from data_contracts.in_mem_source import InMemorySource
from data_contracts.orders import HistoricalRecipeOrders
from data_contracts.preselector.basket_features import historical_preselector_vector
from data_contracts.recipe import (
    NormalizedRecipeFeatures,
    RecipeMainIngredientCategory,
)


@pytest.mark.asyncio
async def test_importance_and_target_vector_computation() -> None:
    """
    Tests if the target and importance vectors behave as intended.
    Meaning

    Importance vector: Higher importance where the aggregated behavior is low in variation over different weeks
    Target vector: Aggregate the mean behavior over weeks
    """
    store = ContractStore.empty()

    store.add_feature_view(HistoricalRecipeOrders)
    store.add_feature_view(NormalizedRecipeFeatures)
    store.add_feature_view(RecipeMainIngredientCategory)

    store = store.update_source_for(
        HistoricalRecipeOrders.location,
        InMemorySource.from_values({
            "agreement_id": [1, 1, 1, 1, 2, 2, 2, 2],
            "recipe_id": [1, 2, 1, 1, 1, 3, 3, 2],
            "company_id": ["a"] * 8,
            "week": [1, 1, 2, 2] * 2,
            "year": [2024] * 8,
            "portion_size": [2] * 8,
        }) # type: ignore
    )
    store = store.update_source_for(
        NormalizedRecipeFeatures.location,
        InMemorySource.from_values({
            "recipe_id": [1, 2, 3],
            "portion_size": [2, 2, 2],
            "company_id": ["a", "a", "a"],
            "normalized_at": [datetime.now(tz=timezone.utc)] * 3,
            "main_recipe_id": [1, 2, 3],
            "year": [1, 1, 1],
            "week": [1, 1, 1],
            "average_rating": [1, 0, 0.5],
            "cost_of_food": [0, 0, 0],
            "number_of_ratings_log": [1, 0.5, 1],
            "cooking_time_from": [0, 1, 0],
            "is_low_cooking_time": [1, 0, 1],
            "is_medium_cooking_time": [0, 0, 0],
            "is_high_cooking_time": [0, 1, 0],
            "is_family_friendly": [0, 0, 1],
            "is_kids_friendly": [0, 0, 1],
            "is_lactose": [1, 1, 0],
            "is_spicy": [0, 0, 0],
            "is_cheep": [0, 0, 0],
            "is_adams_signature": [0, 0, 0],
            "is_gluten_free": [0, 1, 0],
            "is_vegan": [0, 0, 0],
            "is_vegetarian": [0, 0, 0],
            "is_roede": [0, 0, 0],
            "is_chefs_choice": [0, 0, 0],
            "is_low_calorie": [0, 0, 0],
            "is_weight_watchers": [0, 0, 0],
            "is_fish": [0, 0, 0],
            "energy_kcal_per_portion": [0, 0, 0],
            "carbs_pct": [0, 0, 0],
            "fat_pct": [0, 0, 0],
            "fat_saturated_pct": [0, 0, 0],
            "protein_pct": [0, 0, 0],
            "taxonomy_ids": [[1, 2], [1,4], [2, 4, 1]],
            "fruit_veg_fresh_p": [0, 0, 0],
        }) # type: ignore
    )
    store = store.update_source_for(
        RecipeMainIngredientCategory.location,
        InMemorySource.from_values({
            "recipe_id": [1, 2, 3],
            "main_protein_category_id": [1216, 1503, 1128],
            "main_protein_name": ["salmon", "chicken", "beef"],
            "main_carbohydrate_category_id": [1047, 2182, 940],
            "main_carboydrate_name": ["grain", "pasta", "vegs"],
        }) # type: ignore
    )

    dummy_request = RetrivalRequest("", RecipeMainIngredientCategory.location, set(), set(), set())

    lazy_vectors = await historical_preselector_vector(
        dummy_request,
        limit=None,
        from_date=date(year=2024, month=1, day=7 * 3 + 1),
        store=store
    )
    vectors = lazy_vectors.collect()
    assert vectors.height == 4

    first_agreement_target = vectors.filter(
        (pl.col("agreement_id") == 1) & (pl.col("vector_type") == "target")
    ).to_dicts()[0]

    assert first_agreement_target["is_lactose_percentage"] == 1
    assert first_agreement_target["is_vegan_percentage"] == 0

    assert first_agreement_target["cooking_time_mean"] >= 0.1
    assert first_agreement_target["cooking_time_mean"] <= 0.3

    first_agreement_importance = vectors.filter(
        (pl.col("agreement_id") == 1) & (pl.col("vector_type") == "importance")
    ).to_dicts()[0]

    assert first_agreement_importance["is_lactose_percentage"] > first_agreement_importance["cooking_time_mean"]
    assert first_agreement_importance["is_lactose_percentage"] == first_agreement_importance["is_vegan_percentage"]

    second_agreement_target = vectors.filter(
        (pl.col("agreement_id") == 2) & (pl.col("vector_type") == "target")
    ).to_dicts()[0]
    assert second_agreement_target["is_vegan_percentage"] == 0

    assert second_agreement_target["is_lactose_percentage"] >= 0.4
    assert second_agreement_target["is_lactose_percentage"] <= 0.6

    second_agreement_importance = vectors.filter(
        (pl.col("agreement_id") == 2) & (pl.col("vector_type") == "importance")
    ).to_dicts()[0]

    assert second_agreement_importance["is_lactose_percentage"] > second_agreement_importance["cooking_time_mean"]
