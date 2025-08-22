from typing import Optional, Any

from mlflow.pyfunc.model import PythonModel
import pandas as pd
from attribute_scoring.common import ArgsTrain
from attribute_scoring.train.configs import DataConfig, ModelConfig
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

DATA_CONFIG = DataConfig()
MODEL_CONFIG = ModelConfig()


def model(args: ArgsTrain, classifier: Optional[object] = None) -> Pipeline:
    """Builds a machine learning pipeline that preprocesses data and applies a classifier.

    Args:
        args (Args): Configuration arguments.
        classifier (Optional[object]): A pre-built classifier model.

    Returns:
        Pipeline: A scikit-learn Pipeline consisting of preprocessing steps and the classifier.
    """
    preprocessor = create_preprocessor(
        columns_to_encode=DATA_CONFIG.columns_to_encode,
        columns_to_scale=DATA_CONFIG.columns_to_scale,
    )
    if classifier is None:
        classifier = MODEL_CONFIG.classifier(args.company, args.target)

    model_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )

    return model_pipeline


def create_preprocessor(
    columns_to_encode: list[str], columns_to_scale: list[str]
) -> ColumnTransformer:
    """Creates a preprocessor for the machine learning pipeline.

    Args:
        columns_to_encode (list[str]): List of categorical column names to one-hot encode.
        columns_to_scale (list[str]): List of numerical column names to standardize.

    Returns:
        ColumnTransformer: A preprocessor that combines scaling and one-hot encoding.
    """
    scaler = StandardScaler()
    ohe = OneHotEncoder(handle_unknown="ignore")

    preprocessor = ColumnTransformer(
        transformers=[
            ("numerical", Pipeline([("standardizer", scaler)]), columns_to_scale),
            ("onehot", Pipeline([("one_hot_encoder", ohe)]), columns_to_encode),
        ],
        remainder="passthrough",
        sparse_threshold=1,
    )

    return preprocessor


class ModelWrapper(PythonModel):
    """Wrapper for a trained model to return probabilities instead of predictions."""

    def __init__(self, trained_model: Pipeline):
        self.model = trained_model

    def preprocess_result(self, model_input: pd.DataFrame) -> pd.DataFrame:
        return model_input

    def predict(
        self, context: Any, model_input: pd.DataFrame, params: Optional[dict] = None
    ) -> Any:
        processed_df = self.preprocess_result(model_input.copy())
        results = self.model.predict_proba(processed_df)[:, 1]
        return results
