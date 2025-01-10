import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from ml_example_project.train.preprocessor import PreProcessor


class ClassificationPipeline(mlflow.pyfunc.PythonModel):
    """
    I adapted the code from here:
    # https://towardsdatascience.com/explainable-generic-ml-pipeline-with-mlflow-2494ca1b3f96

    Attributes:
        model (BaseEstimator or None): A scikit-learn compatible model instance
        preprocessor (Any or None): Data preprocessing pipeline
        config (Any or None): Optional config for model settings
        task(str): Type of ML task ('classification' or 'regression')
    """

    def __init__(
        self,
        model: any,  # type: ignore
        preprocessor: PreProcessor,
        task: str = "classify",
    ):
        """
        Initialize the Pipeline.

        Parameters:
            model (BaseEstimator, optional):
                - Scikit-learn compatible model
                - Defaults to None
            preprocessor (Any, optional):
                - Transformer or pipeline for data preprocessing
                - Defaults to None
            config (Any, optional):
                - Additional model settings
                - Defaults to None
        """
        self.model = model
        self.preprocessor = preprocessor
        self.task = task

    def fit(
        self,
        X_train: pd.DataFrame,  # noqa
        y_train: pd.Series,
    ) -> None:
        """
        Train the model on provided data.
        - Applies preprocessing to features
        - Fits model on transformed data
        Parameters:
            X_train (pd.DataFrame): Training features
            y_train (pd.Series): Target values
        """
        X_train_preprocessed = self.preprocessor.fit_transform(X_train.copy())  # noqa
        self.model.fit(X_train_preprocessed, y_train)

    def predict(
        self,
        context: any,  # type: ignore
        model_input: pd.DataFrame,
    ) -> np.ndarray:
        """
        Generate predictions using trained model.

        - Applies preprocessing to new data
        - Uses model to make predictions

        Parameters:
            context (Any): Optional context information provided
                by MLflow during the prediction phase
            model_input (pd.DataFrame): Input features

        Returns:
            Any: Model predictions or probabilities
        """
        processed_model_input = self.preprocessor.transform(model_input.copy())
        if self.task == "predict_prob":
            prediction = self.model.predict_proba(processed_model_input)
        elif self.task == "classify":
            prediction = self.model.predict(processed_model_input)
        else:
            raise ValueError(f"Invalid task: {self.task}")
        return prediction


def define_model_pipeline(
    model_params: dict,
    task: str = "classify",
) -> ClassificationPipeline:
    """Define model pipeline."""
    preprocessor = PreProcessor(
        numeric_features=["number_of_ingredients", "number_of_taxonomies"], categorical_features=["cooking_time_from"]
    )

    rf = RandomForestClassifier(**model_params)
    pipeline = ClassificationPipeline(preprocessor=preprocessor, model=rf, task=task)
    return pipeline
