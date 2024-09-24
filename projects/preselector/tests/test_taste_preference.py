import polars as pl
import pytest
from aligned import ContractStore
from data_contracts.in_mem_source import InMemorySource
from preselector.main import filter_out_recipes_based_on_preference


@pytest.fixture()
def model_contracts() -> ContractStore:
    from aligned.feature_source import BatchFeatureSource
    from data_contracts.preselector.store import RecipeNegativePreferences

    store = ContractStore.empty()
    store.add_view(RecipeNegativePreferences)

    locations = {
        RecipeNegativePreferences.location: InMemorySource.from_values({
            "recipe_id":    [1, 2, 3, 4, 5, 6, 7],
            "portion_size": [2, 2, 2, 2, 2, 2, 2],
            "preference_ids": [["a", "b"], [], [], [], ["b"], ["a"], ["c"]],
            "loaded_at": [
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11",
                "2024-10-11T11:11:11"
            ]
        })
    }

    assert isinstance(store.feature_source, BatchFeatureSource)
    assert isinstance(store.feature_source.sources, dict)

    for location, source in locations.items():
        store.feature_source.sources[location.identifier] = source

    return store


@pytest.mark.asyncio()
async def test_remove_recipes_with_taste_preferences(model_contracts: ContractStore) -> None:

    original_recipes = pl.DataFrame({
        "recipe_id": [1, 2, 3, 4, 5, 6, 7],
    })
    filtered = await filter_out_recipes_based_on_preference(
        original_recipes,
        portion_size=2,
        taste_preference_ids=["a"],
        store=model_contracts
    )

    assert filtered.height == 5


@pytest.mark.asyncio()
async def test_remove_recipes_with_taste_preferences_uppercase(model_contracts: ContractStore) -> None:

    original_recipes = pl.DataFrame({
        "recipe_id": [1, 2, 3, 4, 5, 6, 7],
    })
    filtered = await filter_out_recipes_based_on_preference(
        original_recipes,
        portion_size=2,
        taste_preference_ids=["B"],
        store=model_contracts
    )

    assert filtered.height == 5


def test_convert_concept() -> None:
    from preselector.process_stream import GenerateMealkitRequest, convert_concepts_to_attributes

    original_request = GenerateMealkitRequest(
        agreement_id=1,
        company_id="..",
        compute_for=[],
        concept_preference_ids=["4A3E19DF-9524-4308-B927-BD20522628B0"],
        taste_preferences=[],
        portion_size=4,
        number_of_recipes=4,
        override_deviation=False,
        has_data_processing_consent=False
    )

    new_request = convert_concepts_to_attributes(original_request)
    assert len(new_request.concept_preference_ids) == 2
    assert len(new_request.taste_preference_ids) == 3
