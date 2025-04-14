import logging
import random

import numpy as np
import pandas as pd
from reci_pick.helpers import combine_dictionaries, get_dict_values_as_array
from reci_pick.preprocessing import (
    encode_recipe_names,
    get_negative_recipes,
    get_user_embeddings_dict,
    pool_user_embeddings,
)


def get_inputs_for_training(
    df_order_history_train: pd.DataFrame,
    df_menu_recipes: pd.DataFrame,
    id_to_embedding_lookup: dict,
    pooling_method: str,
    training_size: int = 300000,
    is_pad_popular_recipes: bool = True,
    min_recipes_per_user: int = 5,
) -> tuple[np.array, np.array, np.array, dict]:
    """
    Get the inputs for the training data
    """
    df_order_history_train = df_order_history_train[
        df_order_history_train["main_recipe_id"].isin(df_menu_recipes["main_recipe_id"].unique())
    ]

    df_train = create_training_dataframe(
        df_order_history_train=df_order_history_train,
        df_menu_recipes=df_menu_recipes,
    )
    df_train = df_train.head(training_size)
    user_embeddings_pooled_dict = get_user_profile(
        df_order_history=df_order_history_train,
        id_to_embedding_lookup=id_to_embedding_lookup,
        pooling_method=pooling_method,
        is_pooled=True,
        is_pad_popular_recipes=is_pad_popular_recipes,
        min_recipes_per_user=min_recipes_per_user,
    )
    user_embeddings_input = get_dict_values_as_array(
        key_list=df_train["billing_agreement_id"].values, look_up_dict=user_embeddings_pooled_dict
    )

    recipe_embeddings_input = get_dict_values_as_array(
        key_list=df_train["main_recipe_id"].values, look_up_dict=id_to_embedding_lookup
    )

    # target = df_train["is_purchase"].values

    return (user_embeddings_input, recipe_embeddings_input, df_train, user_embeddings_pooled_dict)


def get_recipe_embeddings(df_recipes_processed: pd.DataFrame, recipe_numeric_features: list[str]) -> tuple[dict, dict]:
    """
    Get the recipe embeddings.
    This involves encoding the recipe names and combining the text embeddings with the numeric features.
    Args:
        df_recipes: pd.DataFrame
        recipe_numeric_features: list[str]
    Returns:
        id_to_recipe_embedding_lookup: dict
        id_to_name_lookup: dict
    """

    id_to_recipe_numeric_features_lookup = dict(
        zip(df_recipes_processed["main_recipe_id"], df_recipes_processed[recipe_numeric_features].values)
    )
    logging.info("Encoding recipe names...")
    id_to_text_embedding_lookup, id_to_name_lookup = encode_recipe_names(df_recipes=df_recipes_processed)

    logging.info("Combining text embeddings with numeric features...")
    id_to_recipe_embedding_lookup = combine_dictionaries(
        dict1=id_to_text_embedding_lookup, dict2=id_to_recipe_numeric_features_lookup
    )

    return id_to_recipe_embedding_lookup, id_to_name_lookup


def create_training_dataframe(
    df_order_history_train: pd.DataFrame,
    df_menu_recipes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create the training dataframe.
    This includes:
    - getting the negative recipes per user and sampling the negative recipes.
        For each user, the number of negative recipes to sample is the same as the number of positive recipes
        to make a balanced dataset.
    - Obtaining the positive recipes per user from the purchase history
    """
    # Get the negative recipes per user
    df_user_negative_recipes = get_negative_recipes(
        df_order_history=df_order_history_train, df_menu_recipes=df_menu_recipes
    )

    df_user_negative_recipes["num_negative_recipes_to_sample"] = df_user_negative_recipes[
        ["num_recipes_purchased", "num_recipes_not_purchased"]
    ].min(axis=1)
    # Sample the negative recipes
    df_user_negative_recipes["sampled_negative_recipes"] = df_user_negative_recipes.apply(
        lambda row: random.sample(
            row["recipes_not_purchased"], min(row["num_negative_recipes_to_sample"], len(row["recipes_not_purchased"]))
        )
        if row["recipes_not_purchased"]
        else [],
        axis=1,
    )
    # Use the purchase history directly as the positive part
    df_train_positive = df_order_history_train[["billing_agreement_id", "main_recipe_id"]].drop_duplicates()
    df_train_positive["is_purchase"] = 1
    df_train_negative = df_user_negative_recipes[["billing_agreement_id", "sampled_negative_recipes"]]
    df_train_negative = df_train_negative.explode(column=["sampled_negative_recipes"])
    df_train_negative = df_train_negative.rename(columns={"sampled_negative_recipes": "main_recipe_id"})
    df_train_negative["is_purchase"] = 0

    df_train = pd.concat([df_train_positive, df_train_negative], ignore_index=True)
    # Give it a shuffle
    df_train = df_train.sample(frac=1, random_state=17)
    return df_train


def get_user_profile(
    df_order_history: pd.DataFrame,
    id_to_embedding_lookup: dict,
    pooling_method: str,
    is_pooled: bool = True,
    is_pad_popular_recipes: bool = True,
    min_recipes_per_user: int = 5,
) -> dict:
    """
    Create user profile using recipe embeddings of their purchase history.
    is_pooled: bool = True
        If True, the user embeddings are pooled using the pooling method.
        This is needed for users to have a 1d vector (needed for the NN model).
        However, it can be set to False to use baseline model.
    is_pad_popular_recipes: bool = True
        If True, the users without the minimum number of recipes are padded with
        a few most popular recipes from their concept.
    min_recipes_per_user: int = 5
        The minimum number of recipes per user profile. If user has less than this number,
        they are padded with the most popular recipes from their concept.
    """
    user_embeddings_dict = get_user_embeddings_dict(
        df_order_history=df_order_history,
        id_to_embedding_lookup=id_to_embedding_lookup,
        is_pad_popular_recipes=is_pad_popular_recipes,
        min_recipes_per_user=min_recipes_per_user,
    )
    if is_pooled:
        user_embeddings_dict = pool_user_embeddings(user_embedings_dict=user_embeddings_dict, method=pooling_method)
    return user_embeddings_dict
