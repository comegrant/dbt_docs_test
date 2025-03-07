from datetime import datetime
from random import seed

import polars as pl
import pytest
from aligned import ContractStore, FeatureLocation
from aligned.feature_source import BatchFeatureSource
from aligned.sources.in_mem_source import InMemorySource
from aligned.sources.random_source import RandomDataSource, data_for_request
from concept_definition_app import potential_features
from data_contracts.preselector.basket_features import (
    ImportanceVector,
    PredefinedVectors,
    TargetVectors,
    WeeksSinceRecipe,
)
from data_contracts.preselector.store import Preselector
from numpy.random import seed as np_seed
from preselector.main import run_preselector, run_preselector_for_request
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.store import preselector_store


@pytest.fixture()
def dummy_store() -> ContractStore:
    store = preselector_store()

    return store.dummy_store()


@pytest.mark.asyncio()
async def test_preselector_run_without_user_data(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expect a successful response, but a not a meaningful response.
    """
    seed(1)
    np_seed(1)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4

    recipe_pool = 100

    features = potential_features()

    assert isinstance(dummy_store.feature_source, BatchFeatureSource)
    assert isinstance(dummy_store.feature_source.sources, dict)

    for source in list(dummy_store.feature_source.sources.keys()):
        loc = FeatureLocation.from_string(source)
        if loc.location_type != "feature_view":
            continue

        request = dummy_store.feature_view(loc.name).request
        if "agreement_id" in request.entity_names:
            del dummy_store.feature_source.sources[loc.identifier]

    output = await run_preselector(
        customer=GenerateMealkitRequest(
            agreement_id=0,
            company_id="some-id",
            compute_for=[YearWeek(week=week, year=year)],
            concept_preference_ids=["some-id"],
            taste_preferences=[],
            portion_size=portion_size,
            number_of_recipes=number_of_recipes,
            override_deviation=False,
            has_data_processing_consent=False,
        ),
        available_recipes=pl.DataFrame(
            {
                "recipe_id": list(range(recipe_pool)),
                "menu_week": [week] * recipe_pool,
                "menu_year": [year] * recipe_pool,
                "main_recipe_id": list(range(recipe_pool)),
                "variation_id": ["a"] * recipe_pool,
                "product_id": ["a"] * recipe_pool,
                "variation_portions": [4] * recipe_pool,
            }
        ),
        target_vector=pl.DataFrame({feat.name: 0.5 for feat in features}),
        importance_vector=pl.DataFrame({feat.name: 0.5 for feat in features}),
        recommendations=pl.DataFrame(),
        selected_recipes={1: year * 100 + week - 3},
        store=dummy_store,
    )

    assert len(output.main_recipe_ids) == number_of_recipes


@pytest.mark.asyncio()
async def test_preselector_run(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expect a successful response, but a not a meaningful response.
    """
    seed(1)
    np_seed(1)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4

    recipe_pool = 100

    features = potential_features()

    dummy_store = dummy_store.update_source_for(
        WeeksSinceRecipe.location,
        InMemorySource(
            pl.DataFrame(
                data={
                    "agreement_id": list(range(recipe_pool)),
                    "recipe_id": list(range(recipe_pool)),
                    "company_id": ["dd"] * recipe_pool,
                    "main_recipe_id": [1] * recipe_pool,
                    "last_order_year_week": [year * 100 + week - 3] * recipe_pool,
                    "from_year_week": [year * 100 + week - 3] * recipe_pool,
                }
            )
        ),
    )

    output = await run_preselector(
        customer=GenerateMealkitRequest(
            agreement_id=1,
            company_id="some-id",
            compute_for=[YearWeek(week=week, year=year)],
            concept_preference_ids=["some-id"],
            taste_preferences=[],
            portion_size=portion_size,
            number_of_recipes=number_of_recipes,
            override_deviation=False,
            has_data_processing_consent=False,
        ),
        available_recipes=pl.DataFrame(
            {
                "recipe_id": list(range(recipe_pool)),
                "menu_week": [week] * recipe_pool,
                "menu_year": [year] * recipe_pool,
                "main_recipe_id": list(range(recipe_pool)),
                "variation_id": ["a"] * recipe_pool,
                "product_id": ["a"] * recipe_pool,
                "variation_portions": [4] * recipe_pool,
            }
        ),
        target_vector=pl.DataFrame({feat.name: 0.5 for feat in features}),
        importance_vector=pl.DataFrame({feat.name: 0.5 for feat in features}),
        recommendations=pl.DataFrame(),
        selected_recipes={1: year * 100 + week - 3},
        store=dummy_store,
    )

    assert len(output.main_recipe_ids) == number_of_recipes


@pytest.mark.asyncio()
async def test_preselector_end_to_end(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expect a successful response, but a not a meaningful response.
    """
    from data_contracts.preselector.menu import PreselectorYearWeekMenu

    seed(3)
    np_seed(3)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4
    iso_now = datetime.now().isoformat()  # noqa: DTZ005
    agreement_id = 100
    concept_id = "my-concept-id".upper()
    company_id = "some-company_id".upper()
    recipe_pool = 100

    # Removing the sources that the pre-selector source have not defined
    # Would be nice for something with better support for this.
    source = dummy_store.feature_source
    preselector_deps = Preselector.metadata.source.depends_on().union({FeatureLocation.model("rec_engine")})

    assert isinstance(source, BatchFeatureSource)
    assert isinstance(source.sources, dict)

    new_sources = source.sources.copy()

    for source_identifier in source.sources:
        loc = FeatureLocation.from_string(source_identifier)
        if loc not in preselector_deps:
            del new_sources[source_identifier]

    source.sources = new_sources

    request = GenerateMealkitRequest(
        agreement_id=agreement_id,
        company_id=company_id,
        compute_for=[YearWeek(week=week, year=year)],
        concept_preference_ids=[concept_id],
        taste_preferences=[],
        portion_size=portion_size,
        number_of_recipes=number_of_recipes,
        override_deviation=False,
        has_data_processing_consent=True,
    )

    target_data = (await data_for_request(TargetVectors.query().request, size=1)).with_columns(
        agreement_id=pl.lit(agreement_id)
    )

    predefined_sample = await data_for_request(PredefinedVectors.query().request, size=1)

    defined_vectors = predefined_sample.with_columns(
        vector_type=pl.lit("importance"), concept_id=pl.lit(concept_id), company_id=pl.lit(company_id)
    ).vstack(
        predefined_sample.with_columns(
            vector_type=pl.lit("target"), concept_id=pl.lit(concept_id), company_id=pl.lit(company_id)
        )
    )

    store = (
        dummy_store.update_source_for(
            PreselectorYearWeekMenu.location,
            RandomDataSource.with_values(
                {
                    "recipe_id": list(range(recipe_pool)),
                    "portion_id": [1] * recipe_pool,
                    "loaded_at": [iso_now] * recipe_pool,
                    "menu_week": [week] * recipe_pool,
                    "menu_year": [year] * recipe_pool,
                    "menu_recipe_order": list(range(recipe_pool)),
                    "main_recipe_id": list(range(recipe_pool)),
                    "variation_id": ["some-id"] * recipe_pool,
                    "product_id": ["some_id"] * recipe_pool,
                    "variation_portions": [portion_size] * recipe_pool,
                    "company_id": [company_id] * recipe_pool,
                }
            ),
        )
        .update_source_for(TargetVectors.location, RandomDataSource(partial_data=target_data))
        .update_source_for(ImportanceVector.location, RandomDataSource(partial_data=target_data))
        .update_source_for(PredefinedVectors.location, RandomDataSource(partial_data=defined_vectors))
        .update_source_for(
            WeeksSinceRecipe.location,
            InMemorySource(
                pl.DataFrame(
                    data={
                        "agreement_id": list(range(recipe_pool)),
                        "recipe_id": list(range(recipe_pool)),
                        "company_id": ["dd"] * recipe_pool,
                        "main_recipe_id": [1] * recipe_pool,
                        "last_order_year_week": [year * 100 + week - 3] * recipe_pool,
                        "from_year_week": [year * 100 + week - 3] * recipe_pool,
                    }
                )
            ),
        )
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
