from random import seed

import polars as pl
import pytest
from aligned import ContractStore
from aligned.data_source.batch_data_source import DummyDataSource
from aligned.feature_source import BatchFeatureSource
from concept_definition_app import potential_features
from numpy.random import seed as np_seed
from preselector.main import run_preselector
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.store import preselector_store


@pytest.fixture()
def dummy_store() -> ContractStore:
    store = preselector_store()

    assert isinstance(store.feature_source, BatchFeatureSource)
    assert isinstance(store.feature_source.sources, dict)

    for source_name in store.feature_source.sources:
        store.feature_source.sources[source_name] = DummyDataSource()

    return store


@pytest.mark.asyncio()
async def test_preselector_run(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expecte a successful response, but a not a meaningful response.
    """
    seed(1)
    np_seed(1)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4

    features = potential_features()

    main_recipe_ids, _ = await run_preselector(
        customer=GenerateMealkitRequest(
            agreement_id=0,
            company_id="some-id",
            compute_for=[YearWeek(week=week, year=year)],
            concept_preference_ids=["some-id"],
            taste_preferences=[],
            portion_size=portion_size,
            number_of_recipes=number_of_recipes,
            override_deviation=False,
            has_data_processing_consent=False
        ),
        available_recipes=pl.DataFrame({
            "recipe_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "menu_week": [week] * 10,
            "menu_year": [year] * 10,
            "main_recipe_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "variation_id": ["a"] * 10,
            "product_id": ["a"] * 10,
            "variation_portions": [4] * 10
        }),
        target_vector=pl.DataFrame({
            feat.name: 0.5
            for feat in features
        }),
        importance_vector=pl.DataFrame({
            feat.name: 0.5
            for feat in features
        }),
        recommendations=pl.DataFrame(),
        store=dummy_store
    )

    assert len(main_recipe_ids) == number_of_recipes
