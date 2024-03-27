import logging
from datetime import UTC, datetime
from pathlib import Path

import mlflow
import pandas as pd

from customer_churn.data.preprocess import Preprocessor
from customer_churn.models.logistic_regression import LogisticRegression

logger = logging.getLogger(__name__)


def train_model(
    features: pd.DataFrame,
    company_code: str,
    write_to: Path,
    forecast_weeks: int = 4,
    mlflow_tracking_uri: str | None = None,
    experiment_name: str | None = None
) -> None:
    logger.info("Make predictions")
    logger.info(features.columns)

    if mlflow_tracking_uri:
        train_model_with_mlflow(
            features,
            company_code=company_code,
            forecast_weeks=forecast_weeks,
            mlflow_tracking_uri=mlflow_tracking_uri,
            experiment_name=experiment_name,
        )
    else:
        train_model_locally(
            features,
            company_code=company_code,
            write_to=write_to,
            forecast_weeks=forecast_weeks,
        )


def train_model_locally(features: pd.DataFrame, company_code: str, write_to: Path, forecast_weeks: int = 4) -> None:
    # Generate training data
    df_x_train, df_y_train, df_x_val, df_y_val = Preprocessor().prep_training_data(
        features,
    )

    model = LogisticRegression(
        forecast_weeks=forecast_weeks,
    )

    # Train model
    model.fit(df_x_train, df_y_train, df_x_val, df_y_val)

    # Save model
    runtime = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    model.save(
        local_path=write_to,
        model_filename=f"customer_churn_model_{company_code}_{runtime}.pkl",
    )


def train_model_with_mlflow(
    features: pd.DataFrame,
    company_code: str,
    forecast_weeks: int = 4,
    mlflow_tracking_uri: str | None = None,
    experiment_name: str | None = None
) -> None:

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name=experiment_name)

    with mlflow.start_run() as mlflow_run:
        df_x_train, df_y_train, df_x_val, df_y_val = Preprocessor().prep_training_data(
            features,
        )

        mlflow.log_param("forecast_weeks", forecast_weeks)
        mlflow.sklearn.autolog()

        model = LogisticRegression(
            forecast_weeks=forecast_weeks,
        )

        # Train model
        model.fit(df_x_train, df_y_train, df_x_val, df_y_val)

        run_id = mlflow_run.info.run_uuid
        logger.info("Resource ID (Run ID): %s" % run_id)
        logger.info("Experiment ID: %s" % mlflow_run.info.experiment_id)

        # Save model
        runtime = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
        model.save(
            local_path=Path.cwd() / "models",
            model_filename=f"customer_churn_model_{company_code}_{runtime}.pkl",
        )
        mlflow.register_model(f"runs:/{run_id}/model", "customer_churn_model_%s" % company_code)
