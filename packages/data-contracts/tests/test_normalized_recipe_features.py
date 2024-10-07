
import pytest
from aligned import ContractStore
from aligned.feature_source import BatchFeatureSource
from aligned.sources.random_source import RandomDataSource
from data_contracts.recipe import (
    NormalizedRecipeFeatures,
    RecipeCost,
    RecipeFeatures,
    RecipeNutrition,
    compute_normalized_features,
)
from data_contracts.recommendations.store import recommendation_feature_contracts


@pytest.fixture()
def dummy_store() -> ContractStore:
    store = recommendation_feature_contracts()

    assert isinstance(store.feature_source, BatchFeatureSource)
    assert isinstance(store.feature_source.sources, dict)

    for source_name in store.feature_source.sources:
        store.feature_source.sources[source_name] = RandomDataSource()

    return store

@pytest.mark.asyncio
async def test_normalize_features_logic(dummy_store: ContractStore) -> None:

    store = dummy_store.update_source_for(
        RecipeNutrition.location,
        RandomDataSource.with_values({
            "recipe_id": [1, 1, 2, 2, 3, 3],
            "portion_size": [2, 4] * 3
        })
    ).update_source_for(
        RecipeCost.location,
        RandomDataSource.with_values({
            "recipe_id": [1, 1, 2, 2, 3, 3],
            "portion_size": [2, 4] * 3
        })
    ).update_source_for(
        RecipeFeatures.location,
        RandomDataSource.with_values({
            "main_recipe_id": [1, 2, 3],
            "recipe_id": [1, 2, 3],
            "year": [2024] * 3,
            "week": [1] * 3,
            "company_id": ["test"] * 3
        })
    )

    request = NormalizedRecipeFeatures.query().request
    test = (await compute_normalized_features(request, None, store)).collect()

    expected_features = request.all_returned_columns
    if request.event_timestamp:
        expected_features.remove(request.event_timestamp.name)

    df = test.select(expected_features)
    assert not df.is_empty()
