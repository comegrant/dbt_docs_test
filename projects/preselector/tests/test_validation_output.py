import polars as pl
from polars.testing import assert_frame_equal
from preselector.output_validation import compliancy_metrics, error_metrics, validation_metrics


def test_compliancy_metrics() -> None:
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
        }
    )

    expected_comp = pl.DataFrame(
        {
            "company_id": pl.Series(["a", "a"], dtype=pl.Utf8),
            "portion_size": pl.Series([4, 2], dtype=pl.Int64),
            "total_records": pl.Series([2, 2], dtype=pl.UInt32),
            "broken_allergen": pl.Series([0, 2], dtype=pl.UInt32),
            "broken_preference": pl.Series([1, 0], dtype=pl.UInt32),
            "percentage_allergen": pl.Series([0.0, 100.0], dtype=pl.Float64),
            "percentage_preference": pl.Series([50.0, 0.0], dtype=pl.Float64),
            "compliancy_error": pl.Series([False, True], dtype=pl.Boolean),
            "compliancy_warning": pl.Series([True, False], dtype=pl.Boolean),
            "agreement_id_broken_allergen": pl.Series([[[]], [[1, 3]]], dtype=pl.List(pl.List(pl.Int64))),
            "agreement_id_broken_preference": pl.Series([[[2]], [[]]], dtype=pl.List(pl.List(pl.Int64))),
        }
    )

    expected_error = pl.DataFrame(
        {
            "company_id": pl.Series(["a", "a"], dtype=pl.Utf8),
            "portion_size": pl.Series([4, 2], dtype=pl.Int64),
            "total_records": pl.Series([2, 2], dtype=pl.UInt32),
            "broken_mean_ordered_ago": pl.Series([1, 0], dtype=pl.UInt32),
            "percentage_mean_ordered_ago": pl.Series([50.0, 0.0], dtype=pl.Float64),
            "broken_avg_error": pl.Series([1, 1], dtype=pl.UInt32),
            "percentage_avg_error": pl.Series([50.0, 50.0], dtype=pl.Float64),
            "broken_acc_error": pl.Series([1, 1], dtype=pl.UInt32),
            "percentage_acc_error": pl.Series([50.0, 50.0], dtype=pl.Float64),
            "vector_error": pl.Series([True, False], dtype=pl.Boolean),
            "vector_warning": pl.Series([True, True], dtype=pl.Boolean),
            "agreement_id_mean_ordered_ago": pl.Series([[[4]], [[]]], dtype=pl.List(pl.List(pl.Int64))),
            "agreement_id_avg_error": pl.Series([[[4]], [[3]]], dtype=pl.List(pl.List(pl.Int64))),
            "agreement_id_acc_error": pl.Series([[[4]], [[3]]], dtype=pl.List(pl.List(pl.Int64))),
        }
    )

    result_comp = compliancy_metrics(df)
    result_error = error_metrics(df)

    assert_frame_equal(expected_comp, result_comp)
    assert_frame_equal(expected_error, result_error)


def test_validation_metric() -> None:
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
        }
    )

    comliancy_df = compliancy_metrics(df)
    error_df = error_metrics(df)

    metrics = validation_metrics(comliancy_df, error_df)
    exp_total, exp_comp_warnings, exp_comp_errors, exp_err_warning, exp_err_error = 2, 1, 1, 2, 1

    assert metrics["total_checks"] == exp_total
    assert metrics["comliancy_warnings"] == exp_comp_warnings
    assert metrics["comliancy_errors"] == exp_comp_errors
    assert metrics["vector_warnings"] == exp_err_warning
    assert metrics["vector_errors"] == exp_err_error
