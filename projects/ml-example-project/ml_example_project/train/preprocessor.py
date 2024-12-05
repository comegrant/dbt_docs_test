import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


class PreProcessor(BaseEstimator, TransformerMixin):
    """
    I based the code from here:
    https://towardsdatascience.com/explainable-generic-ml-pipeline-with-mlflow-2494ca1b3f96s
    - Handles scaling of numeric data
    - Performs imputation of missing values
    Attributes:
        transformer (ColumnTransformer): ColumnTransformer for preprocessing
        numeric_features (List[str]): Names of numeric features
        categorical_features (List[str]): Names of categorical features
    """

    def __init__(self, numeric_features: list, categorical_features: list):
        """
        Initialize preprocessor.
        - Creates transformer pipeline for numeric and categorical features

        Parameters:
            numeric_features (List[str]): Names of numeric features
            categorical_features (List[str]): Names of categorical features
        """
        self.numeric_features = numeric_features
        self.categorical_features = categorical_features
        self.transformer = None

    def fit(self, X, y=None): # noqa since it's convention and the function must have these params
        """
        Fits the transformer on the provided dataset.
        - Configures scaling for numeric features
        - Sets up imputation for missing values
        - Configures encoding for categorical features

        Parameters:
            X (pd.DataFrame): The input features to fit the transformer.
            y (pd.Series, optional): Target variable, not used in this method.
        """
        # Infer passthrough features

        numeric_transformer = Pipeline(steps=[
            ('scaler', StandardScaler())
        ])

        categorical_transformer = Pipeline(steps=[
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ])

        self.transformer = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, self.numeric_features),
                ('cat', categorical_transformer, self.categorical_features),
            ],
            remainder="passthrough"
        )

        self.transformer.fit(X)
        return self

    def transform(self, X): # noqa since using X is a convention
        """
        Transform input data using fitted pipeline.

        - Applies scaling to numeric features
        - Handles missing values through imputation

        Parameters:
            X (pd.DataFrame): Input features to transform

        Returns:
            pd.DataFrame: Transformed data with scaled and imputed features
        """
        return self.transformer.transform(X)

    def fit_transform(
        self,
        X: pd.DataFrame, # noqa
        y: pd.Series = None
    ) -> pd.DataFrame:
        """
        Fits the transformer on the input data and then transforms it.

        Parameters:
            X (pd.DataFrame): The input features to fit and transform.
            y (pd.Series, optional): Target variable, not used in this method.

        Returns:
            pd.DataFrame: The transformed data.
        """
        self.fit(X, y)
        return self.transform(X)

    def get_feature_names_out(self) -> list[str]:
        """
        Get feature names after transformation.

        Returns:
            List[str]: List of feature names after transformation.
        """
        feature_names = self.transformer.get_feature_names_out()

        return feature_names
