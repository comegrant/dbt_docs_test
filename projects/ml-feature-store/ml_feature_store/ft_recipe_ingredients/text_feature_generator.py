import numpy as np
import pandas as pd

from ml_feature_store.ft_recipe_ingredients.text_column_processor import add_nouns_only_and_lemmatized_columns


def get_generic_name_lemmatized_nouns(
    df_recipe_ingredients: pd.DataFrame,
    df_distinct_ingredients: pd.DataFrame,
    distinct_ingredient_text_col_name: str,
    distinct_ingredient_id_column_name: str,
    nouns_only_column_name: str,
    lemmatized_column_name: str,
) -> pd.DataFrame:
    """Add a column that replace the generic ingredient name list with only lemmatized nouns

    Args:
        df_recipe_ingredients (pd.DataFrame):
            the table that contains
                - recipe_id
                - aggregated generic_ingredient_ids as str
        df_distinct_ingredients (pd.DataFrame):
            the table that contains:
                - generic_ingredient_id
                - generic_ingredient_name
            NOT aggregated
        distinct_ingredient_text_col_name (str): the column name of the text column
            in df_distinct_ingredients. i.e., generic_ingredient_name
        distinct_ingredient_id_column_name (str): the column name in
            df_distinct_ingredients that contains the ingredient_id.
                i.e., generic_ingredient_id
        nouns_only_column_name (str): the added column name for the cleaned names
            with nouns only
        lemmatized_column_name (str): the added column name for the cleaned names
            with lemmatized nouns only

    Returns:
        pd.DataFrame: the df_recipe_ingredients with the a new added column
            lemmatized_column_name
    """
    df_lemmatized_nouns = add_nouns_only_and_lemmatized_columns(
        df_distinct_ingredients=df_distinct_ingredients,
        text_column_name=distinct_ingredient_text_col_name,
        nouns_only_column_name=nouns_only_column_name,
        lemmatized_column_name=lemmatized_column_name,
    )
    df_with_clean_name = add_clean_ingredient_names(
        df_recipe_ingredients=df_recipe_ingredients,
        df_ingredient_id_to_cleaned_names=df_lemmatized_nouns,
        clean_text_column_name=lemmatized_column_name,
        id_column_name=distinct_ingredient_id_column_name
    )
    df_with_clean_name[lemmatized_column_name] = df_with_clean_name[lemmatized_column_name].apply(
        lambda x: ",".join(x)
    )
    return df_with_clean_name


def get_dict(a_dict: dict, key: any) -> any:
    try:
        return a_dict.get(key)
    except ValueError:
        return None


def create_id_to_text_mapping(
    df: pd.DataFrame,
    id_column_name: str,
    text_column_name: str
) -> dict:
    id_to_text_dict = df.set_index(id_column_name)[text_column_name].to_dict()
    return id_to_text_dict


def add_clean_ingredient_names(
    df_recipe_ingredients: pd.DataFrame,
    df_ingredient_id_to_cleaned_names: pd.DataFrame,
    clean_text_column_name: str,
    id_column_name: str = "generic_ingredient_id",
) -> pd.DataFrame:
    df_recipe_ingredients["generic_ingredient_id_array"] = (
        df_recipe_ingredients["generic_ingredient_id_list"]
        .str
        .split(",")
        .apply(lambda x: [int(i) for i in x])
    )
    df_recipe_ingredients[clean_text_column_name] = ""
    for language_id in df_recipe_ingredients["language_id"].unique():
        row_index = df_recipe_ingredients[
            df_recipe_ingredients["language_id"] == language_id
        ].index
        df_ingredient_id_to_cleaned_names_one_language = df_ingredient_id_to_cleaned_names[
            df_ingredient_id_to_cleaned_names["language_id"] == language_id
        ]
        mapping_dict = create_id_to_text_mapping(
            df=df_ingredient_id_to_cleaned_names_one_language,
            id_column_name=id_column_name,
            text_column_name=clean_text_column_name,
        )

        vectorize_get_dict = np.vectorize(get_dict)
        df_recipe_ingredients.loc[row_index, clean_text_column_name] = (
            df_recipe_ingredients
            .loc[row_index, "generic_ingredient_id_array"]
            .apply(lambda x: vectorize_get_dict(mapping_dict, x)) # noqa
        )
    df_recipe_ingredients = df_recipe_ingredients.drop(columns="generic_ingredient_id_array")
    return df_recipe_ingredients
