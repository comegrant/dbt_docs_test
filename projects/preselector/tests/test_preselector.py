from datetime import datetime
from random import seed

import polars as pl
import pytest
from aligned import ContractStore
from aligned.data_source.batch_data_source import DummyDataSource, data_for_request
from aligned.feature_source import BatchFeatureSource
from concept_definition_app import potential_features
from data_contracts.in_mem_source import InMemorySource
from data_contracts.preselector.basket_features import ImportanceVector, PredefinedVectors, TargetVectors
from numpy.random import seed as np_seed
from preselector.main import run_preselector, run_preselector_for_request
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

@pytest.mark.asyncio()
async def test_preselector_end_to_end(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expecte a successful response, but a not a meaningful response.
    """
    from data_contracts.preselector.menu import PreselectorYearWeekMenu

    seed(3)
    np_seed(3)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4
    iso_now = datetime.now().isoformat() # noqa: DTZ005
    agreement_id = 100
    concept_id = "my-concept-id".upper()
    company_id = "some-company_id".upper()


    request = GenerateMealkitRequest(
        agreement_id=agreement_id,
        company_id=company_id,
        compute_for=[YearWeek(week=week, year=year)],
        concept_preference_ids=[concept_id],
        taste_preferences=[],
        portion_size=portion_size,
        number_of_recipes=number_of_recipes,
        override_deviation=False,
        has_data_processing_consent=True
    )

    target_data = (await data_for_request(
        TargetVectors.query().request,
        size=1
    )).with_columns(
        agreement_id=pl.lit(agreement_id)
    )

    predefined_sample = await data_for_request(
        PredefinedVectors.query().request,
        size=1
    )

    defined_vectors = predefined_sample.with_columns(
        vector_type=pl.lit("importance"),
        concept_id=pl.lit(concept_id),
        company_id=pl.lit(company_id)
    ).vstack(
        predefined_sample.with_columns(
            vector_type=pl.lit("target"),
            concept_id=pl.lit(concept_id),
            company_id=pl.lit(company_id)
        )
    )

    store = dummy_store.update_source_for(
        PreselectorYearWeekMenu.location,
        InMemorySource.from_values({
            "recipe_id": [1, 2, 3],
            "portion_id": [1, 2, 3],
            "loaded_at": [iso_now] * 3,
            "menu_week": [week] * 3,
            "menu_year": [year] * 3,
            "menu_recipe_order": [1, 2, 3],
            "main_recipe_id": [1, 2, 3],
            "variation_id": ["some-id"] * 3,
            "product_id": ["some_id"] * 3,
            "variation_portions": [portion_size] * 3,
            "company_id": [company_id] * 3,
        })
    ).update_source_for(
        TargetVectors.location,
        InMemorySource(target_data)
    ).update_source_for(
        ImportanceVector.location,
        InMemorySource(target_data)
    ).update_source_for(
        PredefinedVectors.location,
        InMemorySource(defined_vectors)
    )

    response = await run_preselector_for_request(request, store)
    assert response.success, f"Expected a successful response, but got {response}"

    request.has_data_processing_consent = False
    response = await run_preselector_for_request(request, store)
    assert response.success, f"Expected a successful response, but got {response}"

    # Should fail since there are no 1 size portions
    request.portion_size = 1
    response = await run_preselector_for_request(request, store)
    assert response.failures, f"Expected a failure response, but got {response}"
