import pytest
from aligned import ContractStore
from aligned.sources.in_mem_source import InMemorySource
from data_contracts.orders import WeeksSinceRecipe
from preselector.quarantining import compute_weeks_ago


@pytest.mark.asyncio
async def test_weeks_ago_computation() -> None:
    store = ContractStore.empty()
    store.add_view(WeeksSinceRecipe)

    company_id = "Test"
    agreement_id = 123
    main_recipe_ids = [1, 2, 3, 4, 5]

    store = store.update_source_for(
        WeeksSinceRecipe.location,
        InMemorySource.from_values(
            {
                "agreement_id": [agreement_id, agreement_id, 11],
                "company_id": [company_id, company_id, company_id],
                "main_recipe_id": [1, 3, 5],
                "last_order_year_week": [202501, 202505, 202501],
            }
        ),
    )

    ordered_ago = await compute_weeks_ago(
        company_id, agreement_id, year_week=202501, main_recipe_ids=main_recipe_ids, store=store
    )

    column = "ordered_weeks_ago"
    assert ordered_ago.height == len(main_recipe_ids)
    assert ordered_ago[column].null_count() == 3
