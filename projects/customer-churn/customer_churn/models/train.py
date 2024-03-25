import logging

import pandas as pd

from customer_churn.data.preprocess import Preprocessor
from customer_churn.models.logistic_regression import LogisticRegression

logger = logging.getLogger(__name__)


def train_model(
    features: pd.DataFrame,
    company_name: str,
    forecast_weeks: int = 4,
    model_version: str = "1",
) -> None:
    logger.info("Make predictions")
    logger.info(features.columns)
    # Generate training data
    df_x_train, df_y_train, df_x_val, df_y_val = Preprocessor().prep_training_data(
        features,
    )

    logger.info(df_x_train)

    logger.info(df_x_val)

    model = LogisticRegression(
        forecast_weeks=forecast_weeks,
        model_version=model_version,
        company_name=company_name,
    )

    return model.fit(df_x_train, df_y_train, df_x_val, df_y_val)
