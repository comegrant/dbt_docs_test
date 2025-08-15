import polars as pl
import pytest
from aligned import ContractStore
from aligned.sources.random_source import RandomDataSource
from data_contracts.recipe import RecipeMainIngredientCategory
from preselector.output_validation import compliancy_metrics, error_metrics, validation_metrics, variation_metrics
from preselector.store import preselector_store


@pytest.fixture
def dummy_store() -> ContractStore:
    store = preselector_store()

    return store.dummy_store()


# def test_compliancy_metrics() -> None:
#     df = pl.DataFrame(
#         {
#             "company_id": ["a", "a", "a", "a"],
#             "menu_year": [2024, 2024, 2024, 2024],
#             "menu_week": [1, 1, 1, 1],
#             "billing_agreement_id": [1, 2, 3, 4],
#             "portion_size": [2, 4, 2, 4],
#             "compliancy": [1, 2, 1, 3],
#             "error_vector": {
#                 "is_dim_1": [0.001, 0.001, 0.0, 0.0],
#                 "is_dim2": [0.0, 0.0, 0.4, 0.0],
#                 "mean_ordered_ago": [0.0, 0.0, 0.0, 0.03],
#             },
#         }
#     )
#
#     expected_comp = pl.DataFrame(
#         {
#             "company_id": pl.Series(["a", "a"], dtype=pl.Utf8),
#             "portion_size": pl.Series([4, 2], dtype=pl.Int64),
#             "total_records": pl.Series([2, 2], dtype=pl.UInt32),
#             "broken_allergen": pl.Series([0, 2], dtype=pl.UInt32),
#             "broken_preference": pl.Series([1, 0], dtype=pl.UInt32),
#             "percentage_allergen": pl.Series([0.0, 100.0], dtype=pl.Float64),
#             "percentage_preference": pl.Series([50.0, 0.0], dtype=pl.Float64),
#             "compliancy_error": pl.Series([False, True], dtype=pl.Boolean),
#             "compliancy_warning": pl.Series([True, False], dtype=pl.Boolean),
#             "agreement_id_broken_allergen": pl.Series([[[]], [[1, 3]]], dtype=pl.List(pl.List(pl.Int64))),
#             "agreement_id_broken_preference": pl.Series([[[2]], [[]]], dtype=pl.List(pl.List(pl.Int64))),
#         }
#     )
#
#     expected_error = pl.DataFrame(
#         {
#             "company_id": pl.Series(["a", "a"], dtype=pl.Utf8),
#             "portion_size": pl.Series([4, 2], dtype=pl.Int64),
#             "total_records": pl.Series([2, 2], dtype=pl.UInt32),
#             "broken_mean_ordered_ago": pl.Series([1, 0], dtype=pl.UInt32),
#             "percentage_mean_ordered_ago": pl.Series([50.0, 0.0], dtype=pl.Float64),
#             "broken_avg_error": pl.Series([1, 1], dtype=pl.UInt32),
#             "percentage_avg_error": pl.Series([50.0, 50.0], dtype=pl.Float64),
#             "broken_acc_error": pl.Series([1, 1], dtype=pl.UInt32),
#             "percentage_acc_error": pl.Series([50.0, 50.0], dtype=pl.Float64),
#             "vector_error": pl.Series([True, False], dtype=pl.Boolean),
#             "vector_warning": pl.Series([True, True], dtype=pl.Boolean),
#             "agreement_id_mean_ordered_ago": pl.Series([[[4]], [[]]], dtype=pl.List(pl.List(pl.Int64))),
#             "agreement_id_avg_error": pl.Series([[[4]], [[3]]], dtype=pl.List(pl.List(pl.Int64))),
#             "agreement_id_acc_error": pl.Series([[[4]], [[3]]], dtype=pl.List(pl.List(pl.Int64))),
#         }
#     )
#
#     result_comp = compliancy_metrics(df)
#     result_error = error_metrics(df)
#
#     assert_frame_equal(expected_comp, result_comp)
#     assert_frame_equal(expected_error, result_error)


@pytest.mark.asyncio
async def test_validation_metric(dummy_store: ContractStore) -> None:
    df = pl.DataFrame(
        {
            "company_id": ["a", "a", "a", "a"],
            "menu_year": [2024, 2024, 2024, 2024],
            "menu_week": [1, 1, 1, 1],
            "billing_agreement_id": [1, 2, 3, 4],
            "portion_size": [2, 4, 2, 4],
            "compliancy": [1, 2, 1, 3],
            "error_vector": {
                "is_dim_1": [0.001, 0.001, 0.0, 0.0],
                "is_dim2": [0.0, 0.0, 0.4, 0.0],
                "mean_ordered_ago": [0.0, 0.0, 0.0, 0.03],
            },
            "number_of_recipes": [4, 4, 4, 4],
            "main_recipe_ids": [
                ["63059", "51311", "44106", "86774"],
                ["63059", "51311", "44106", "86774"],
                ["63059", "51311", "44106", "86774"],
                ["63059", "51311", "44106", "86774"],
            ],
        }
    )

    dummy_store = dummy_store.update_source_for(
        RecipeMainIngredientCategory.location,
        RandomDataSource.with_values(
            {
                "recipe_id": [63059, 51311, 44106, 86774],
                "main_protein_category_id": [1, 1, 3, 4],
                "main_carbohydrate_category_id": [5, 5, 5, 6],
            }
        ),
    )

    comliancy_df = compliancy_metrics(df)
    error_df = error_metrics(df)
    variation_df = await variation_metrics(df, dummy_store)
    metrics = validation_metrics(comliancy_df, error_df, variation_df)
    exp_total = 4
    exp_allergen_error = 50.0
    exp_preference_error = 25.0
    exp_mean_ordered_ago_error = 25.0
    exp_avg_error = 50.0
    exp_acc_error = 25.0
    exp_carb_warnings = 100.0
    exp_protein_warnings = 0.0

    assert metrics["sum_total_records"] == exp_total
    assert metrics["perc_broken_allergen"] == exp_allergen_error
    assert metrics["perc_broken_preference"] == exp_preference_error
    assert metrics["perc_broken_mean_ordered_ago"] == exp_mean_ordered_ago_error
    assert metrics["perc_broken_avg_error"] == exp_avg_error
    assert metrics["perc_broken_acc_error"] == exp_acc_error
    assert metrics["perc_carb_warnings"] == exp_carb_warnings
    assert metrics["perc_protein_warnings"] == exp_protein_warnings
