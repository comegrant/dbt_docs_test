from datetime import datetime
from random import seed

import numpy as np
import polars as pl
import pytest
from aligned import ContractStore, FeatureLocation
from aligned.schemas.feature import Feature
from aligned.sources.in_mem_source import InMemorySource
from aligned.sources.random_source import RandomDataSource, data_for_request
from data_contracts.preselector.basket_features import (
    BasketFeatures,
    ImportanceVector,
    NormalizedRecipeFeatures,
    PredefinedVectors,
    TargetVectors,
    WeeksSinceRecipe,
)
from data_contracts.preselector.store import Preselector
from data_contracts.recipe import RecipeEmbedding, RecipeMainIngredientCategory, RecipeNegativePreferences
from data_contracts.recipe_vote import RecipeVote
from numpy.random import seed as np_seed
from preselector.main import run_preselector, run_preselector_for_request
from preselector.preview_server_store import remove_user_sources
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.store import preselector_store
from pytest_mock import MockFixture


def potential_features() -> list[Feature]:
    return list(BasketFeatures.compile().request_all.needed_requests[0].all_features)


@pytest.fixture()
def dummy_store() -> ContractStore:
    store = preselector_store()

    return store.dummy_store()


@pytest.mark.asyncio()
async def test_preselector_run_without_user_data(dummy_store: ContractStore, mocker: MockFixture) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expect a successful response, but a not a meaningful response.
    """
    from preselector import main

    seed(1)
    np_seed(1)

    number_of_recipes = 3
    week = 10
    year = 2024
    portion_size = 4

    recipe_pool = 100

    features = potential_features()
    dummy_store, _ = remove_user_sources(dummy_store)

    search_spy = mocker.spy(main, "find_best_combination")

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

    search_spy.assert_called_once()
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
    preselector_deps = Preselector.metadata.source.depends_on().union({FeatureLocation.model("rec_engine")})

    new_sources = dummy_store.sources.copy()

    for loc in dummy_store.sources:
        if loc not in preselector_deps:
            del new_sources[loc]

    dummy_store.sources = new_sources

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
            PreselectorYearWeekMenu,
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
        .update_source_for(TargetVectors, RandomDataSource(partial_data=target_data))
        .update_source_for(ImportanceVector, RandomDataSource(partial_data=target_data))
        .update_source_for(PredefinedVectors, RandomDataSource(partial_data=defined_vectors))
        .update_source_for(
            RecipeMainIngredientCategory,
            RandomDataSource.with_values(
                {"recipe_id": list(range(recipe_pool)), "main_carbohydrate_category_id": [1] * recipe_pool}
            ),
        )
        .update_source_for(
            WeeksSinceRecipe,
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


@pytest.mark.asyncio()
async def test_preselector_quarantining(dummy_store: ContractStore) -> None:
    """
    Tests that all data is processed in the expected way.
    Therefore, we expected a successful response, but a not a meaningful response.
    """
    from data_contracts.preselector.menu import PreselectorYearWeekMenu

    # Do not remove!! As this will make the tests reproducible
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
    recipe_pool = number_of_recipes + 2

    # Removing the sources that the pre-selector source have not defined
    # Would be nice for something with better support for this.
    preselector_deps = Preselector.metadata.source.depends_on().union({FeatureLocation.model("rec_engine")})

    new_sources = dummy_store.sources.copy()

    for loc in dummy_store.sources:
        if loc not in preselector_deps:
            del new_sources[loc]

    dummy_store.sources = new_sources

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

    vector_features = BasketFeatures.query().request.request_result.feature_columns

    target_data = (
        (await data_for_request(TargetVectors.query().request, size=1))
        .with_columns(
            agreement_id=pl.lit(agreement_id),
            is_roede_percentage=pl.lit(0),
            is_low_calorie=pl.lit(0),
        )
        .with_columns(pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features)
    )

    predefined_sample = (
        (await data_for_request(PredefinedVectors.query().request, size=1))
        .with_columns(
            is_roede_percentage=pl.lit(0),
            is_low_calorie=pl.lit(0),
        )
        .with_columns(pl.col(feat) / pl.sum_horizontal(vector_features) for feat in vector_features)
    )
    defined_vectors = predefined_sample.with_columns(
        vector_type=pl.lit("importance"), concept_id=pl.lit(concept_id), company_id=pl.lit(company_id)
    ).vstack(
        predefined_sample.with_columns(
            vector_type=pl.lit("target"), concept_id=pl.lit(concept_id), company_id=pl.lit(company_id)
        )
    )

    def normalise_embedding(emb: pl.Series) -> list[float]:
        """
        Make the semantic recipe embeddings irrelevant for this test, by making all embeddings the same.

        NB: Important that the length is 1, as we use the dot product for similarity.
        """
        return (np.ones(emb.shape) / emb.shape[0]).tolist()

    store = (
        dummy_store.update_source_for(
            PreselectorYearWeekMenu,
            RandomDataSource.with_values(
                {
                    "recipe_id": list(range(recipe_pool)),
                    "portion_id": [1] * recipe_pool,
                    "loaded_at": [iso_now] * recipe_pool,
                    "menu_week": [week] * recipe_pool,
                    "menu_year": [year] * recipe_pool,
                    "main_recipe_id": list(range(recipe_pool)),
                    "menu_recipe_order": list(range(recipe_pool)),
                    "variation_id": ["some-id"] * recipe_pool,
                    "product_id": ["some_id"] * recipe_pool,
                    "variation_portions": [portion_size] * recipe_pool,
                    "company_id": [company_id] * recipe_pool,
                }
            ),
        )
        .update_source_for(TargetVectors, RandomDataSource(partial_data=target_data))
        .update_source_for(ImportanceVector, RandomDataSource(partial_data=target_data))
        .update_source_for(PredefinedVectors, RandomDataSource(partial_data=defined_vectors))
        .update_source_for(
            WeeksSinceRecipe,
            InMemorySource(
                pl.DataFrame(
                    data={
                        "agreement_id": [agreement_id, agreement_id],
                        "recipe_id": [1, 2],
                        "company_id": [company_id] * 2,
                        "main_recipe_id": [1, 2],
                        "last_order_year_week": [year * 100 + week] * 2,
                        "from_year_week": [year * 100 + week] * 2,
                    }
                )
            ),
        )
        .update_source_for(
            NormalizedRecipeFeatures,
            RandomDataSource.with_values(
                {
                    "recipe_id": list(range(recipe_pool)),
                    "portion_size": [request.portion_size] * recipe_pool,
                    "company_id": [company_id] * recipe_pool,
                    "is_adams_signature": [False] * recipe_pool,
                    "is_cheep": [False] * recipe_pool,
                    "is_weight_watchers": [False] * recipe_pool,
                    "is_slow_grown_chicken": [False] * recipe_pool,
                    "is_low_calorie": [False] * recipe_pool,
                    "is_roede": [False] * recipe_pool,
                    "main_ingredient_id": [1] * recipe_pool,
                }
            ),
        )
        .update_source_for(
            RecipeEmbedding,
            RandomDataSource().transform_with_polars(
                lambda df: df.with_columns(
                    pl.col("embedding").map_elements(normalise_embedding, return_dtype=pl.List(pl.Float32()))
                )
            ),
        )
        .update_source_for(
            RecipeNegativePreferences,
            RandomDataSource.with_values(
                {
                    "recipe_id": list(range(recipe_pool)),
                    "portion_size": [request.portion_size] * recipe_pool,
                    "preference_ids": [[]] * recipe_pool,
                }
            ),
        )
        .update_source_for(
            RecipeMainIngredientCategory,
            RandomDataSource.with_values(
                {
                    "recipe_id": list(range(recipe_pool)),
                    "main_protein_category_id": [1] * recipe_pool,
                    "main_carbohydrate_category_id": [1] * recipe_pool,
                }
            ),
        )
        .update_source_for(
            RecipeVote,
            InMemorySource(
                pl.DataFrame(
                    data={
                        "main_recipe_id": list(range(recipe_pool)),
                        "agreement_id": [agreement_id] * recipe_pool,
                        "is_dislike": [False] * recipe_pool,
                        "is_favorite": [False] * recipe_pool,
                    }
                )
            ),
        )
    )

    response = await run_preselector_for_request(request, store)
    assert response.success, f"Expected a successful response, but got {response}"

    selected_ids = response.success[0].main_recipe_ids
    assert 1 not in selected_ids
    assert 2 not in selected_ids
