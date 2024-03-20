import mlflow
import pandas as pd

from customer_churn.models.logistic_regression import LogisticRegression


async def make_predictions(
    features: pd.DataFrame,
    pretrained_model: str | None = None,
) -> None:
    model = LogisticRegression(forecast_weeks=features.forecast_weeks)

    if pretrained_model:
        mlflow.set_tracking_uri(pretrained_model)
        model.load(pretrained_model)

    return model.predict(features)
