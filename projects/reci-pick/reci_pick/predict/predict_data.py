from datetime import date

import numpy as np
import pandas as pd
from reci_pick.helpers import get_dict_values_as_array
from reci_pick.preprocessing import get_user_concept_dishes, pool_user_embeddings
from reci_pick.train.training_data import get_user_profile
from time_machine.forecasting import get_forecast_calendar


def get_user_embeddings(
    df_order_history: pd.DataFrame,
    df_user_preferences: pd.DataFrame,
    id_to_recipe_embedding_lookup: dict,
    top_n_per_user: int = 5,
    top_n_per_concept: int = 8,
    look_back_weeks: int = 24,
    pooling_method: str = "mean",
) -> dict:
    df_non_cold_start_users_order_history = df_order_history[
        df_order_history["billing_agreement_id"].isin(df_user_preferences["billing_agreement_id"].unique())
    ]
    non_cold_start_user_embeddings_pooled_dict = get_user_profile(
        df_order_history=df_non_cold_start_users_order_history,
        id_to_embedding_lookup=id_to_recipe_embedding_lookup,
        pooling_method=pooling_method,
        is_pooled=True,
        is_pad_popular_recipes=True,
        min_recipes_per_user=5,
    )

    df_cold_start_users_preferences = df_user_preferences[
        ~df_user_preferences["billing_agreement_id"].isin(df_order_history["billing_agreement_id"].unique())
    ][["billing_agreement_id", "concept_combination_list"]]

    cold_start_user_embeddings_pooled_dict = get_cold_start_users_embeddings(
        df_non_cold_start_order_history=df_order_history,
        df_cold_start_user_preferences=df_cold_start_users_preferences,
        id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
        top_n_per_user=top_n_per_user,
        top_n_per_concept=top_n_per_concept,
        look_back_weeks=look_back_weeks,
        pooling_method=pooling_method,
    )
    users_embeddings = {**non_cold_start_user_embeddings_pooled_dict, **cold_start_user_embeddings_pooled_dict}
    return users_embeddings


def get_cold_start_users_embeddings(
    df_non_cold_start_order_history: pd.DataFrame,
    df_cold_start_user_preferences: pd.DataFrame,
    id_to_recipe_embedding_lookup: dict,
    top_n_per_user: int = 5,
    top_n_per_concept: int = 8,
    look_back_weeks: int = 24,
    pooling_method: str = "mean",
) -> dict:
    # Calculate the most popular recipes per concept,
    # to be used as padding for cold start users
    df_cold_start_users_concept_dishes = get_user_concept_dishes(
        df_order_history_train=df_non_cold_start_order_history,
        top_n_per_user=top_n_per_user,
        top_n_per_concept=top_n_per_concept,
        look_back_weeks=look_back_weeks,
        df_user_preferences=df_cold_start_user_preferences,
    )

    cold_start_user_embedings_dict = {}
    for i in df_cold_start_users_concept_dishes.itertuples():
        user_embeddings = get_dict_values_as_array(
            look_up_dict=id_to_recipe_embedding_lookup, key_list=i.popular_concept_recipe_ids
        )
        cold_start_user_embedings_dict[i.billing_agreement_id] = user_embeddings
    cold_start_user_embedings_dict_pooled = pool_user_embeddings(
        user_embedings_dict=cold_start_user_embedings_dict, method=pooling_method
    )

    return cold_start_user_embedings_dict_pooled


def get_menu_to_predict(
    df_menu_recipes: pd.DataFrame,
    prediction_date: str | None,
    cut_off_day: int,
    num_weeks: int = 4,
) -> pd.DataFrame:
    """
    Get the menu to predict.
    This is defined as starting from the first week that is after the cut-off day of prediction_date.
    """
    if (prediction_date is None) or (prediction_date == ""):
        prediction_date = date.today()
    else:
        prediction_date = date.fromisoformat(prediction_date)
    df_weeks_to_predict = get_forecast_calendar(
        num_weeks=num_weeks, cut_off_day=cut_off_day, forecast_date=prediction_date
    )
    df_weeks_to_predict = df_weeks_to_predict.rename(
        columns={
            "year": "menu_year",
            "week": "menu_week",
        }
    )
    menus_to_predict = df_weeks_to_predict.merge(df_menu_recipes, how="left")
    return menus_to_predict


def divide_users_into_chunks(
    user_id_list: list[int],
    num_chunks: int | None = 20,
) -> np.array:
    user_id_arr = np.array(user_id_list)
    user_chunks = np.array_split(user_id_arr, num_chunks)
    return user_chunks
