import polars as pl
import pytest
from aligned import ContractStore
from aligned.sources.in_mem_source import InMemorySource
from data_contracts.orders import WeeksSinceRecipe


@pytest.mark.asyncio
async def test_weeks_ago_features() -> None:
    store = ContractStore.empty()
    store.add(WeeksSinceRecipe)

    agreements = [1, 1, 2, 2]
    expected_week_diff = [-1, 2, -6, 0]

    store = store.update_source_for(
        WeeksSinceRecipe.location,
        InMemorySource.from_values(
            {
                "agreement_id": agreements,
                "company_id": ["a"] * len(agreements),
                "main_recipe_id": [1, 2, 1, 2],
                "last_order_year_week": [202444, 202451, 202501, 202510],
            }
        ),
    )

    output = (
        await store.feature_view(WeeksSinceRecipe)
        .features_for(
            {
                "agreement_id": agreements,
                "company_id": ["a"] * len(agreements),
                "main_recipe_id": [1, 2, 1, 2],
                "from_year_week": [202443, 202501, 202447, 202510],
            }
        )
        .to_polars()
    )

    assert output["order_diff"].cast(pl.Int32).to_list() == expected_week_diff
