import logging

import pandas as pd
from constants.companies import Company

from customer_churn.models.logistic_regression import LogisticRegression

logger = logging.getLogger(__name__)


def make_predictions(
    features: pd.DataFrame,
    company: Company,
    forecast_weeks: int = 4,
    model_version: str = "1",
) -> None:
    logger.info("Make predictions")
    logger.info(features.columns)
    model = LogisticRegression(forecast_weeks=forecast_weeks)

    # Load model mlflow or a local path
    model.load(
        model_filename="customer_churn_model_%s" % company.company_code,
        mlflow_model_version=model_version,
    )

    # args.model / f"{args.model}_{args.mlflow_model_version}"

    return model.predict(features)
