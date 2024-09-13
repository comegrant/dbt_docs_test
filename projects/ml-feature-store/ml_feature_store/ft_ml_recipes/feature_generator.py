import pandas as pd
from sklearn.preprocessing import StandardScaler

from ml_feature_store.ft_ml_recipes.configs.taxonomy_config import TAXONOMY_ONESUB_MAPPING, TaxonomyOneSubMapping


def generate_number_of_ingredients(df: pd.DataFrame) -> pd.DataFrame:
    df["number_of_ingredients"] = df["chef_ingredient_id_list"].apply(
        lambda x: len(x.split(",")) if isinstance(x, str) else 0
    )
    return df


def generate_number_of_taxonomies(df: pd.DataFrame) -> pd.DataFrame:
    df["number_of_taxonomies"] = df["taxonomy_list"].apply(lambda x: len(x.split(",")) if isinstance(x, str) else 0)
    return df


def generate_encoding_main_ingredient(df: pd.DataFrame) -> pd.DataFrame:
    df["recipe_main_ingredient_id"] = df["recipe_main_ingredient_id"].fillna(-1).astype(int)
    df = pd.get_dummies(df, columns=["recipe_main_ingredient_id"]).drop(
        columns=["recipe_main_ingredient_id_-1"], errors="ignore"
    )
    return df


def generate_normalized_mean_cooking_time(df: pd.DataFrame) -> pd.DataFrame:
    df["cooking_time_mean"] = (df["cooking_time_from"] + df["cooking_time_to"]) / 2

    scaler = StandardScaler()
    df["cooking_time_normalized"] = scaler.fit_transform(df["cooking_time_mean"].values.reshape(-1, 1)).ravel()
    return df


def generate_boolean_taxonomy_attributes(
    df: pd.DataFrame, mapping: TaxonomyOneSubMapping = TAXONOMY_ONESUB_MAPPING
) -> pd.DataFrame:
    def process_row(row: pd.Series) -> pd.Series:
        if pd.isna(row["taxonomy_list"]):
            taxonomy_list = []
        else:
            taxonomy_list = [taxonomy.strip().lower() for taxonomy in row["taxonomy_list"].split(",")]

        feature_flags = {}
        company_id = row["company_id"]

        for category, company_keywords in mapping.model_dump().items():
            feature_flags[f"has_{category}_taxonomy"] = company_id in company_keywords and any(
                keyword in taxonomy_list for keyword in company_keywords[company_id]
            )

        return pd.Series(feature_flags)

    feature_columns = df.apply(process_row, axis=1)
    df_with_features = pd.concat([df, feature_columns], axis=1)

    return df_with_features
