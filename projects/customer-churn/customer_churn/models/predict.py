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
    model = LogisticRegression(forecast_weeks=forecast_weeks)

    model.load(model_filename=f"{company_name}_CHURN", model_version=model_version)

    return model.predict(features)
