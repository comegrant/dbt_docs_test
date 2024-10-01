from random import seed

import pytest
from aligned import ContractStore
from aligned.data_source.batch_data_source import DummyDataSource
from aligned.feature_source import BatchFeatureSource
from data_contracts.recipe import NormalizedRecipeFeatures, compute_normalized_features
from data_contracts.recommendations.store import recommendation_feature_contracts
from numpy.random import seed as np_seed


@pytest.fixture()
def dummy_store() -> ContractStore:
    store = recommendation_feature_contracts()

    assert isinstance(store.feature_source, BatchFeatureSource)
    assert isinstance(store.feature_source.sources, dict)

    for source_name in store.feature_source.sources:
        store.feature_source.sources[source_name] = DummyDataSource()

    return store

@pytest.mark.asyncio
async def test_normalize_features_logic(dummy_store: ContractStore) -> None:

    seed_nr = 1
    seed(seed_nr)
    np_seed(seed_nr)

    request = NormalizedRecipeFeatures.query().request
    test = (await compute_normalized_features(request, None, dummy_store)).collect()

    expected_features = request.all_returned_columns
    if request.event_timestamp:
        expected_features.remove(request.event_timestamp.name)

    df = test.select(expected_features)
    assert not df.is_empty()
