import logging
import pickle
from pathlib import Path

import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient
from sklearn.linear_model import LogisticRegression as SKLogisticRegression
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split

from customer_churn.constants import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    MLFLOW_TRACKING_URI,
)

logger = logging.getLogger(__name__)


class LogisticRegression:
    """Model: Logistic Regression"""

    CUSTOMER_ID_LABEL: str = "agreement_id"

    def __init__(
        self,
        forecast_weeks: int = 4,
        probability_threshold: float = 0.3,
        model_version: str = "1",
        company_name: str = "LMK",
    ):
        """
        :param data: Pandas Dataframe (merged dataset coming from gen.py)
        :probability_threshold: Probability threshold for model estimator
        :param config:
        """
        self.probability_threshold = probability_threshold
        self.forecast_weeks = forecast_weeks

        self.model = self.load_mlflow_model(model_name=f"{company_name}_CHURN", version=model_version)

    def load_mlflow_model(self, model_name: str, version: str = "1") -> mlflow.sklearn.Model | None:
        w = WorkspaceClient(
            host=DATABRICKS_HOST,
            token=DATABRICKS_TOKEN,
        )

        mlflow.login(backend=MLFLOW_TRACKING_URI)

        model = mlflow.artifacts.download_artifacts(
            artifact_uri=w.model_registry.get_model_version_download_uri(model_name, version).artifact_uri,
        )

        if model:
            return mlflow.sklearn.load_model(model)

        return None

    def fit(
        self,
        df_x: pd.DataFrame,
        df_y: pd.DataFrame,
        df_x_val: pd.DataFrame,
        df_y_val: pd.DataFrame,
        save_model_filename: str | None = None,
    ) -> bool:
        """
        Main function for model fit
        :save_model_filename: Default None. If given model will be saved to given path/name (pickle format)
        :return:
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

        model = SKLogisticRegression(random_state=0, max_iter=1000)
        model.fit(x_train, y_train)

        logger.info(f"Training features columns: {x_train.columns!s}")
        y_val_pred = (model.predict_proba(df_x_val)[:, 1] >= self.probability_threshold).astype(int)

        precision, recall, fscore, _ = score(df_y_val, y_val_pred, average="macro")
        logger.info(f"precision: {precision}")
        logger.info(f"recall: {recall}")
        logger.info(f"f1-score: {fscore}")

        if save_model_filename:
            logger.info("Saving model to: " + save_model_filename)
            with Path.open(save_model_filename, "wb") as file:
                pickle.dump(model, file)
            logger.info("Model saved!")

        return True

    def load(
        self,
        model_obj: SKLogisticRegression | None,
        model_filename: str | None = None,
    ) -> None:
        """
        Model loading
        :param model_filename: model filename / path
        :return:
        """
        logger.info(f"Loading model from file: {model_filename}")
        if model_obj is not None:
            self.model = model_obj
            return

        with Path.open(model_filename, "rb") as file:
            self.model = pickle.load(file)

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
