import logging

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def get_metrics(
    df_pred: pd.Series,
    df_actual: pd.Series,
) -> dict:
    logger.info(df_actual)
    logger.info(df_pred)
    df_metrics = pd.concat([df_actual, df_pred], axis=1)
    df_metrics.columns = ["actual", "predicted"]
    df_metrics["error"] = df_metrics["actual"] - df_metrics["predicted"]
    df_metrics["abs_error"] = abs(df_metrics["error"])
    df_metrics["abs_error_pct"] = df_metrics["abs_error"] / df_actual
    df_metrics["error_pct"] = df_metrics["error"] / df_actual
    mape = df_metrics["abs_error_pct"].mean()
    mae = df_metrics["abs_error"].mean()

    metric_dict = {"mape": mape, "mae": mae, "df_metrics": df_metrics}

    return metric_dict


def get_metrics_for_prediction(
    df_merged: DataFrame, pred_col: str, actual_col: str
) -> dict:
    target_col = pred_col[5:]
    df_merged = (
        df_merged.withColumn(f"{target_col}_error", f.col(pred_col) - f.col(actual_col))
        .withColumn(f"{target_col}_abs_error", f.abs(f.col(f"{target_col}_error")))
        .withColumn(
            f"{target_col}_abs_error_pct",
            f.col(f"{target_col}_abs_error") / f.col(actual_col),
        )
        .withColumn(
            f"{target_col}_error_pct", f.col(f"{target_col}_error") / f.col(actual_col)
        )
    )

    mape = df_merged.agg({f"{target_col}_abs_error_pct": "mean"}).collect()[0][0]
    mae = df_merged.agg({f"{target_col}_abs_error": "mean"}).collect()[0][0]

    metric_dict = {f"{target_col}_mape": mape, f"{pred_col}_mae": mae}

    return df_merged, metric_dict


def evaluate_predictions(
    df_actual_values: DataFrame, df_predictions: DataFrame
) -> tuple[DataFrame, dict]:
    df_merged = df_actual_values.join(
        df_predictions,
        on=[
            "year",
            "week",
            "company_id",
        ],
        how="left",
    )

    df_merged, metric_num_total_orders = get_metrics_for_prediction(
        df_merged, "pred_num_total_orders", "actual_num_total_orders"
    )
    df_merged, metric_num_dishes_orders = get_metrics_for_prediction(
        df_merged, "pred_num_dishes_orders", "actual_num_dishes_orders"
    )
    df_merged, metric_num_dishes_orders_calc = get_metrics_for_prediction(
        df_merged, "pred_num_dishes_orders_calc", "actual_num_dishes_orders"
    )
    df_merged, metric_num_dishes_orders_avg = get_metrics_for_prediction(
        df_merged, "pred_num_dishes_orders_avg", "actual_num_dishes_orders"
    )
    df_merged, metric_perc_dishes_orders = get_metrics_for_prediction(
        df_merged, "pred_perc_dishes_orders", "actual_perc_dishes_orders"
    )

    dct_metrics = (
        metric_num_total_orders
        | metric_num_dishes_orders
        | metric_num_dishes_orders_calc
        | metric_num_dishes_orders_avg
        | metric_perc_dishes_orders
    )

    return df_merged, dct_metrics
