import pandas as pd


def get_metrics(
    df_predictions: pd.DataFrame,
    prediction_col: str,
    actual_col: str,
) -> dict:
    df_metrics = df_predictions.copy()
    df_metrics["abs_error"] = abs(df_metrics[prediction_col] - df_metrics[actual_col])
    df_metrics["abs_error_pct"] = df_metrics["abs_error"] / df_metrics[actual_col]
    mae = df_metrics["abs_error"].mean()
    mape = df_metrics["abs_error_pct"].mean()
    return {"mae": mae, "mape": mape, "df_metrics": df_metrics}
