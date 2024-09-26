from typing import Optional

import pandas as pd


def add_category_features(
    df_recipe_ingredients: pd.DataFrame,
    category_mappings: dict,
    binary_column_prefix: Optional[str] = "has_"
) -> pd.DataFrame:
    """According to the categories config, add binary columns
        binary column have value 1 if the category id contains
            at least one of the values in the maping
    Args:
        df_recipe_ingredients (pd.DataFrame): the dataframe with column
            ingredient_category_id_list

    Returns:
        pd.DataFrame: df_recipe_ingredients with the additional binary columns
    """
    df_recipe_ingredients["ingredient_category_id_array"] = (
        df_recipe_ingredients["ingredient_category_id_list"]
        .str
        .split(",")
        .apply(lambda x: [int(i) for i in x])
    )
    for category_name, category_list in category_mappings.items():
        binary_column_name = binary_column_prefix + category_name
        mask = df_recipe_ingredients["ingredient_category_id_array"].apply(
            lambda x: any(item in x for item in category_list) # noqa
        )
        df_recipe_ingredients[binary_column_name] = mask.astype(int)

    df_recipe_ingredients = df_recipe_ingredients.drop(
        columns=["ingredient_category_id_array"])

    return df_recipe_ingredients
