from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from dishes_forecasting.train.training_set import get_training_pk_target


def get_test_metrics(
    spark: SparkSession,
    env: str,
    company_id: str,
    X_test: pd.DataFrame, # noqa
    y_pred_transformed: pd.Series,
    min_yyyyww: int,
    max_yyyyww: Optional[int],
) -> tuple[pd.DataFrame, float]:
    df_training_target = get_training_pk_target(
        spark=spark,
        env=env,
        company_id=company_id,
        min_yyyyww=min_yyyyww,
        max_yyyyww=max_yyyyww,
        is_training_set=False,
    )

    df_test = df_training_target.loc[X_test.index]
    df_test["y_pred_transformed"] = y_pred_transformed
    df_test, mae, mape = compute_metrics(df_test=df_test)
    df_test_binned = get_test_metrics_binned(
        df_test=df_test,
        bins=[0, 100, 200, 300, 400, 500, float('inf')]
    )
    return df_test, mae, mape, df_test_binned


def compute_metrics(
    df_test: pd.DataFrame
) -> tuple[pd.DataFrame, float, float]:
    df_test["product_variation_qty_pred"] = (
        df_test["y_pred_transformed"] * df_test["total_weekly_qty"]
    ).round()
    df_test["error"] = (
        df_test["product_variation_qty_pred"] - df_test["product_variation_quantity"]
    )
    df_test["abs_error"] = abs(df_test["error"])
    df_test["abs_error_pct"] = df_test["abs_error"] / df_test["product_variation_quantity"]
    mae = df_test["abs_error"].mean()
    mape = df_test["abs_error_pct"].mean()
    return df_test, mae, mape


def get_test_metrics_binned(
    df_test: pd.DataFrame,
    bins: Optional[list] = None,
) -> pd.DataFrame:
    if bins is None:
        bins = [0, 100, 200, 300, 400, 500, float('inf')]
    labels = [
        (
            str(val)
            + "-"
            + str(bins[index+1])
        ) for index, val in enumerate(bins[:-1])
    ]
    df_test['quantity_bin'] = pd.cut(
        df_test['product_variation_quantity'],
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    df_test_binned = df_test.groupby('quantity_bin').agg({
        'abs_error': ['mean', 'median'],
        'abs_error_pct': ['mean', 'median'],
        'product_variation_quantity': 'count'
    }).reset_index()
    return df_test_binned
