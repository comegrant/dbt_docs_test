import logging

import pandas as pd

from customer_churn.models.logistic_regression import LogisticRegression

logger = logging.getLogger(__name__)


def make_predictions(
    features: pd.DataFrame,
    company_name: str,
    forecast_weeks: int = 4,
    model_version: str = "1",
) -> None:
    logger.info("Make predictions")
    logger.info(features.columns)
    model = LogisticRegression(forecast_weeks=forecast_weeks, model_version=model_version, company_name=company_name)

    return model.predict(features)
