import logging
import os
import pickle
from pathlib import Path

import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient
from sklearn.linear_model import LogisticRegression as SKLogisticRegression
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split

from customer_churn.env import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
)

logger = logging.getLogger(__name__)


class LogisticRegression:
    """Model: Logistic Regression"""

    CUSTOMER_ID_LABEL: str = "agreement_id"

    def __init__(self, forecast_weeks: int = 4, probability_threshold: float = 0.3):
        """
        :param data: Pandas Dataframe (merged dataset coming from gen.py)
        :probability_threshold: Probability threshold for model estimator
        :param config:
        """
        self.probability_threshold = probability_threshold
        self.forecast_weeks = forecast_weeks

    def fit(
        self,
        df_x: pd.DataFrame,
        df_y: pd.DataFrame,
        df_x_val: pd.DataFrame,
        df_y_val: pd.DataFrame,
    ) -> bool:
        """
        Main function for model fit
        """
        logger.info("Fitting/Training the model..")

        x_train, x_test, y_train, y_test = train_test_split(
            df_x,
            df_y,
            test_size=0.2,
            random_state=42,
        )

        # Dimensions of the train-test set
        logger.info(f"train {x_train.shape}, test {x_test.shape}")

        # Class distribution between train-test set
        logger.info(
            f"Class distribution in training set: {y_train.value_counts(normalize=True)}",
        )
        logger.info(
            "Class distribution in test set: %s" % y_test.value_counts(normalize=True),
        )

        self.model = SKLogisticRegression(random_state=0, max_iter=1000)
        self.model.fit(x_train, y_train)

        logger.info(f"Training features columns: {x_train.columns!s}")
        y_val_pred = (
            self.model.predict_proba(df_x_val)[:, 1] >= self.probability_threshold
        ).astype(int)

        precision, recall, fscore, _ = score(df_y_val, y_val_pred, average="macro")
        logger.info(f"precision: {precision}")
        logger.info(f"recall: {recall}")
        logger.info(f"f1-score: {fscore}")

        return True

    def save(
        self,
        local_path: Path,
        model_filename: str,
        external_path: Path | None = None,
    ) -> None:
        logger.info("Saving model to: " + model_filename)
        if not local_path.exists():
            logger.info(f"Creating folder {local_path} for model")
            local_path.mkdir(parents=True, exist_ok=True)

        with Path.open(local_path / model_filename, "wb") as file:
            pickle.dump(self.model, file)

        if external_path:
            logger.info("Uploading model to: " + external_path)
            mlflow.log_artifact(local_path / model_filename, external_path)
        logger.info("Model saved!")

    def load(
        self,
        model_filename: str,
        mlflow_model_version: str = "1",
        local_path: Path | None = None,
    ) -> None:
        """
        Model loading
        :param model_filename: model filename / path
        :return:
        """
        if local_path:
            all_models = Path.glob(local_path, "*.pkl")
            latest_model = max(all_models, key=os.path.getctime)
            model_filename = local_path / latest_model
            logger.info("Loading model locally from: " + str(model_filename))
            # Find latest trained model from the local path
            with Path.open(model_filename, "rb") as file:
                self.model = pickle.load(file)
        else:
            logger.info(
                f"Loading model from MLflow: {model_filename} version {mlflow_model_version}",
            )
            self.model = self._load_mlflow_model(
                model_name=model_filename,
                version=mlflow_model_version,
            )

    def predict(
        self,
        features: pd.DataFrame,
        customer_id_label: str = CUSTOMER_ID_LABEL,
    ) -> pd.DataFrame:
        """
        Model predictions
        :param df: dataframe with customer for predictions
        :param model_filename: model filename / path
        :return: Dataframe with predictions
        """

        features_without_entity = features.drop(columns=[customer_id_label])
        logger.info(f"Predict features columns: {features.columns!s}")
        pred_proba = self.model.predict_proba(features_without_entity)

        # Original
        predictions_score = [p[1] for p in pred_proba[:,]]

        features["score"] = predictions_score
        features["model_type"] = "original_pred_proba"
        return features

    def _load_mlflow_model(
        self,
        model_name: str,
        version: str = "1",
    ) -> mlflow.sklearn.Model | None:
        w = WorkspaceClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN,
        )

        mlflow.login()

        model_download_uri = w.model_registry.get_model_version_download_uri(
            model_name,
            version,
        )
        logger.info(f"Downloading model from {model_download_uri}")
        model = mlflow.artifacts.download_artifacts(
            artifact_uri=model_download_uri.artifact_uri,
        )

        if model:
            return mlflow.sklearn.load_model(model)

        return None
