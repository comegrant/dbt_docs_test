from datetime import date, datetime, timezone

import polars as pl
import pytest
from aligned import ContractStore
from aligned.request.retrieval_request import RetrievalRequest
from aligned.sources.random_source import RandomDataSource
from data_contracts.attribute_scoring import AttributeScoring
from data_contracts.orders import HistoricalRecipeOrders
from data_contracts.preselector.basket_features import (
    HistoricalCustomerMealkitFeatures,
    historical_customer_mealkit_features,
    historical_preselector_vector,
)
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
    store.add_feature_view(AttributeScoring)
    store.add_feature_view(HistoricalCustomerMealkitFeatures)

    store = store.dummy_store()

    store = store.update_source_for(
        HistoricalRecipeOrders.location,
        RandomDataSource.with_values(
            {
                "agreement_id": [1, 1, 1, 1, 2, 2, 2, 2],
                "recipe_id": [1, 2, 1, 1, 1, 3, 3, 2],
                "company_id": ["a"] * 8,
                "week": [1, 1, 2, 2] * 2,
                "year": [2024] * 8,
                "portion_size": [2] * 8,
            }
        ),  # type: ignore
    )
    store = store.update_source_for(
        NormalizedRecipeFeatures.location,
        RandomDataSource.with_values(
            {
                "recipe_id": [1, 2, 3],
                "portion_size": [2, 2, 2],
                "company_id": ["a", "a", "a"],
                "normalized_at": [datetime.now(tz=timezone.utc)] * 3,
                "main_recipe_id": [1, 2, 3],
                "year": [1, 1, 1],
                "week": [1, 1, 1],
                "cooking_time_from": [0, 1, 0],
                "is_lactose": [1, 1, 0],
                "is_vegan": [0, 0, 0],
            }
        ),  # type: ignore
    )
    store = store.update_source_for(
        RecipeMainIngredientCategory.location,
        RandomDataSource.with_values(
            {
                "recipe_id": [1, 2, 3],
                "main_protein_category_id": [1216, 1503, 1128],
                "main_protein_name": ["salmon", "chicken", "beef"],
                "main_carbohydrate_category_id": [1047, 2182, 940],
                "main_carboydrate_name": ["grain", "pasta", "vegs"],
            }
        ),  # type: ignore
    )

    dummy_request = RetrievalRequest("", RecipeMainIngredientCategory.location, set(), set(), set())

    lazy_vectors = await historical_customer_mealkit_features(
        dummy_request, from_date=date(year=2024, month=1, day=7 * 3 + 1), store=store
    )
    vectors = lazy_vectors.collect()

    assert vectors.unique(["agreement_id", "year", "week"]).height == 4

    store = store.update_source_for(HistoricalCustomerMealkitFeatures.location, RandomDataSource(partial_data=vectors))

    lazy_vectors = await historical_preselector_vector(dummy_request, limit=None, store=store)

    vectors = lazy_vectors.collect()
    assert vectors.unique(["agreement_id", "vector_type"]).height == 4

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
