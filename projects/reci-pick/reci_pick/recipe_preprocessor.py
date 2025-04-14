import pandas as pd
from mlflow.pyfunc import PythonModel
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder


# Create preprocessor pipeline for imputation and normalization
class RecipePreprocessorModel(PythonModel):
    def __init__(
        self,
        impute_and_normalize_columns: list[str],
        impute_and_one_hot_columns: list[str],
        impute_only_columns: list[str],
    ):
        self.preprocessor = ColumnTransformer(
            transformers=[
                (
                    "impute_and_normalize",
                    Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("scaler", MinMaxScaler())]),
                    impute_and_normalize_columns,
                ),
                (
                    "impute_and_onehot",
                    Pipeline(
                        [
                            ("imputer", SimpleImputer(strategy="most_frequent")),
                            ("onehot", OneHotEncoder(handle_unknown="ignore")),
                        ]
                    ),
                    impute_and_one_hot_columns,
                ),
                (
                    "impute_only",
                    Pipeline(
                        [
                            ("imputer", SimpleImputer(strategy="most_frequent")),
                        ]
                    ),
                    impute_only_columns,
                ),
            ],
            remainder="passthrough",
        )

    def strip_prefix(self, column_name_list: list[str], prefix: str) -> list[str]:
        new_names = []
        for column_name in column_name_list:
            if column_name.startswith(prefix):
                start_index = len(prefix)
                new_names.append(column_name[start_index:])
            else:
                new_names.append(column_name)
        return new_names

    def fit(self, df_recipes: pd.DataFrame) -> None:
        self.preprocessor.fit(df_recipes)

    def predict(self, df_recipes: pd.DataFrame) -> tuple[pd.DataFrame, Pipeline]:
        df_recipes_processed = pd.DataFrame(self.preprocessor.transform(df_recipes))
        preprocessor_columns = self.preprocessor.get_feature_names_out()
        df_recipes_processed.columns = preprocessor_columns
        new_column_names = list(df_recipes_processed.columns)
        for prefixes in [
            "impute_and_normalize__",
            "impute_and_onehot__",
            "impute_only__",
            "remainder__",
        ]:
            new_column_names = self.strip_prefix(column_name_list=new_column_names, prefix=prefixes)
        df_recipes_processed.columns = new_column_names

        return df_recipes_processed
