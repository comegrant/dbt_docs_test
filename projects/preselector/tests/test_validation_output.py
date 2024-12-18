import polars as pl
from polars.testing import assert_frame_equal
from preselector.output_validation import compliancy_metrics, validation_metrics


def test_compliancy_metrics() -> None:
    df = pl.DataFrame(
        {
            "company_id": ["a", "a", "a", "a"],
            "menu_year": [2024, 2024, 2024, 2024],
            "menu_week": [1, 1, 1, 1],
            "billing_agreement_id": [1, 2, 3, 4],
            "portion_size": [2, 4, 2, 4],
            "compliancy": [1, 2, 1, 3],
        }
    )

    expected = pl.DataFrame(
        {
            "company_id": pl.Series(["a", "a"], dtype=pl.Utf8),
            "portion_size": pl.Series([4, 2], dtype=pl.Int64),
            "total_records": pl.Series([2, 2], dtype=pl.UInt32),
            "broken_allergen": pl.Series([0, 2], dtype=pl.UInt32),
            "broken_preference": pl.Series([1, 0], dtype=pl.UInt32),
            "percentage_allergen": pl.Series([0.0, 100.0], dtype=pl.Float64),
            "percentage_preference": pl.Series([50.0, 0.0], dtype=pl.Float64),
            "error": pl.Series([False, True], dtype=pl.Boolean),
            "warning": pl.Series([True, False], dtype=pl.Boolean),
            "agreement_id_errors": pl.Series([[[]], [[1, 3]]], dtype=pl.List(pl.List(pl.Int64))),
            "agreement_id_warnings": pl.Series([[[2]], [[]]], dtype=pl.List(pl.List(pl.Int64))),
        }
    )

    result = compliancy_metrics(df)

    assert_frame_equal(expected, result)


def test_validation_metric() -> None:
    df = pl.DataFrame(
        {
            "company_id": ["a", "a", "a", "a"],
            "menu_year": [2024, 2024, 2024, 2024],
            "menu_week": [1, 1, 1, 1],
            "billing_agreement_id": [1, 2, 3, 4],
            "portion_size": [2, 4, 2, 4],
            "compliancy": [1, 2, 1, 3],
        }
    )

    comliancy_df = compliancy_metrics(df)

    res_comp_total, res_comp_warnings, res_comp_errors = validation_metrics(comliancy_df)
    exp_comp_total, exp_comp_warnings, exp_comp_errors = 2, 1, 1

    assert res_comp_total == exp_comp_total
    assert res_comp_warnings == exp_comp_warnings
    assert res_comp_errors == exp_comp_errors
