import pandas as pd

from ml_feature_store.ft_ml_recipes.configs.taxonomy_config import TAXONOMY_ONESUB_MAPPING, TaxonomyOneSubMapping


def generate_mean_cooking_time(df: pd.DataFrame) -> pd.DataFrame:
    df["cooking_time_mean"] = (df["cooking_time_from"] + df["cooking_time_to"]) / 2

    return df


def convert_columns_to_int(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["recipe_main_ingredient_id", "number_of_taxonomies", "number_of_recipe_steps"]
    for col in cols:
        df[col] = df[col].fillna(-1).astype(int)
    return df


def generate_boolean_taxonomy_attributes(
    df: pd.DataFrame, mapping: TaxonomyOneSubMapping = TAXONOMY_ONESUB_MAPPING
) -> pd.DataFrame:
    mapping_dict = mapping.model_dump()

    def process_row(row: pd.Series) -> pd.Series:
        if pd.isna(row["taxonomy_list"]):
            taxonomy_list = []
        else:
            taxonomy_list = [taxonomy.strip().lower() for taxonomy in row["taxonomy_list"].split(",")]

        feature_flags = {}
        company_id = row["company_id"]

        for category, company_keywords in mapping_dict.items():
            if company_id in company_keywords:
                keywords = company_keywords[company_id]
                feature_flags[f"has_{category}_taxonomy"] = any(
                    any(keyword in taxonomy_item for taxonomy_item in taxonomy_list) for keyword in keywords
                )

        return pd.Series(feature_flags)

    feature_columns = df.apply(process_row, axis=1)
    df_with_features = pd.concat([df, feature_columns], axis=1)

    return df_with_features
