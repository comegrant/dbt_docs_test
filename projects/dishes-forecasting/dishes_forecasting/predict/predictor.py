import mlflow
import numpy as np
import pandas as pd


def make_predictions(
    model: mlflow.models.Model,
    df_pred: pd.DataFrame
) -> pd.DataFrame:
    pred = model.predict(df_pred)
    pred_transformed = np.exp(pred)
    df_pred["variation_ratio_prediction"] = pred_transformed

    return df_pred


def normalize_predictions(
    df_predictions: pd.DataFrame,
    normalized_target: float = 1.0
) -> pd.DataFrame:
    df_normalizing_constant = pd.DataFrame(
        df_predictions.groupby(["menu_year", "menu_week"]).variation_ratio_prediction.sum()
    ).reset_index().rename(
        columns={
            "variation_ratio_prediction": "normalizing_constant"
        }
    )
    df_normalizing_constant["normalizing_constant"] = df_normalizing_constant["normalizing_constant"]

    df_predictions = df_predictions.merge(
        df_normalizing_constant,
    )

    df_predictions["variation_ratio_prediction_normalized"] = (
        df_predictions.variation_ratio_prediction / df_predictions.normalizing_constant * normalized_target
    )
    df_predictions = df_predictions.drop(columns=["normalizing_constant", "variation_ratio_prediction"]).rename(
        columns={
            "variation_ratio_prediction_normalized": "variation_ratio_prediction"
        }
    )
    return df_predictions


def modify_new_portions(
    df_predictions: pd.DataFrame,
    is_add_new_portions_on_top: bool = False,
    new_portions: list[int] | None = None,
    new_portions_targets: list[float] | None = None,
) -> pd.DataFrame:
    if new_portions is None:
        raise ValueError("new_portions is required")
    if new_portions_targets is None:
        raise ValueError("new_portions_targets is required")
    if len(new_portions) != len(new_portions_targets):
        raise ValueError("new_portions and new_portions_targets must have the same length")

    df_old_portions = df_predictions[~df_predictions["portion_quantity"].isin(new_portions)]
    old_portions_target = 1 - sum(new_portions_targets)

    if is_add_new_portions_on_top:
        df_old_portions = normalize_predictions(df_old_portions, normalized_target=1)
    else:
        df_old_portions = normalize_predictions(df_old_portions, normalized_target=old_portions_target)

    df_new_portions = []
    for portion, target in zip(new_portions, new_portions_targets):
        df = df_predictions[df_predictions["portion_quantity"] == portion]
        df_normalized = normalize_predictions(df_predictions=df, normalized_target=target)
        df_new_portions.append(df_normalized)

    df_new_portions_concated = pd.concat(df_new_portions, ignore_index=True)
    df_predictions_modified = pd.concat(
        [df_new_portions_concated, df_old_portions], ignore_index=True
    )

    return df_predictions_modified
