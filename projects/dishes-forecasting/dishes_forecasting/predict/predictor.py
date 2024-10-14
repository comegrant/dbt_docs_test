import mlflow
import numpy as np
import pandas as pd


def make_predictions(
    model: mlflow.models.Model,
    df_pred: pd.DataFrame
)-> pd.DataFrame:
    pred = model.predict(df_pred)
    pred_transformed = np.exp(pred)
    df_pred["variation_ratio_prediction"] = pred_transformed

    return df_pred
