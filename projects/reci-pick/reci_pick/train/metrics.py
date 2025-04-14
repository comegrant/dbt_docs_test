import numpy as np
import pandas as pd
import tensorflow as tf
from reci_pick.postprocessing import (
    modify_score_based_on_purchase_history,
    penalize_high_menu_occurance,
)
from reci_pick.predict.predict_data import get_cold_start_users_embeddings
from reci_pick.train.model import predict_recipe_scores


def get_recommendation_precisions(
    df_order_history_train: pd.DataFrame,
    df_order_history_test: pd.DataFrame,
    user_embeddings_pooled_dict: dict,
    id_to_recipe_embedding_lookup: dict,
    df_menu_recipes: pd.DataFrame,
    trained_model: tf.keras.Model,
    num_test_users: int | None = 2000,
) -> tuple[pd.DataFrame, float, float]:
    cold_start_user_embedings_dict_pooled = get_cold_start_test_users_embeddings(
        df_order_history_train=df_order_history_train,
        df_order_history_test=df_order_history_test,
        user_embeddings_pooled_non_cold_start=user_embeddings_pooled_dict,
        id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
    )
    user_embedings_dict_all = {**user_embeddings_pooled_dict, **cold_start_user_embedings_dict_pooled}
    test_users = np.random.choice(df_order_history_test["billing_agreement_id"].unique(), num_test_users)
    menus_to_predict = df_menu_recipes[
        df_menu_recipes["menu_yyyyww"].isin(df_order_history_test["menu_yyyyww"].unique())
    ]

    recipes_to_predict = menus_to_predict["main_recipe_id"].unique()
    score_df = predict_recipe_scores(
        recipe_ids_to_predict=recipes_to_predict,
        user_billing_agreements=test_users,
        user_embeddings_pooled_dict=user_embedings_dict_all,
        id_to_embedding_lookup=id_to_recipe_embedding_lookup,
        model=trained_model,
    )

    df_score_modified = modify_score_based_on_purchase_history(
        score_df_exploded=score_df,
        df_order_history=df_order_history_train,
        bonus_factor=0.25,
        is_map_similar_recipes=False,
        df_similar_recipes=None,
    )

    df_score_modified = penalize_high_menu_occurance(
        df_menu_recipes=df_menu_recipes.copy(),
        df_menu_to_predict=menus_to_predict,
        df_scores=df_score_modified,
        alpha=-1,
        penalization_factor=0.1,
        score_col="score_modified",
    )

    df_menu_scores = menus_to_predict.merge(df_score_modified, how="left")

    df_menu_scores = df_menu_scores.sort_values(
        by=["menu_yyyyww", "billing_agreement_id", "score_modified"], ascending=[True, True, False]
    )
    n = 10
    df_top_n_recommendations = df_menu_scores.groupby(
        ["menu_year", "menu_week", "menu_yyyyww", "billing_agreement_id"]
    ).head(n)

    df_recs_agged = (
        pd.DataFrame(
            df_top_n_recommendations.groupby(["menu_year", "menu_week", "billing_agreement_id"])["main_recipe_id"].agg(
                list
            )
        )
        .reset_index()
        .rename(columns={"main_recipe_id": "top_n_recipe_ids"})
    )
    df_precision = compute_precision(df_order_history_test=df_order_history_test, df_recommendations=df_recs_agged)
    cold_start_precision = df_precision[
        df_precision["billing_agreement_id"].isin(cold_start_user_embedings_dict_pooled)
    ]["num_purchased_recommendations"].mean()
    non_cold_start_precision = df_precision[
        ~df_precision["billing_agreement_id"].isin(cold_start_user_embedings_dict_pooled)
    ]["num_purchased_recommendations"].mean()
    df_precision_agged_by_week = pd.DataFrame(
        df_precision.groupby(["menu_year", "menu_week"])["num_purchased_recommendations"].mean()
    ).reset_index()
    df_precision_agged_by_week["menu_yyyyww"] = (
        df_precision_agged_by_week["menu_year"].astype(int) * 100
    ) + df_precision_agged_by_week["menu_week"].astype(int)

    return df_precision_agged_by_week, cold_start_precision, non_cold_start_precision


def get_cold_start_test_users_embeddings(
    df_order_history_train: pd.DataFrame,
    df_order_history_test: pd.DataFrame,
    user_embeddings_pooled_non_cold_start: dict,
    id_to_recipe_embedding_lookup: dict,
) -> dict:
    users = df_order_history_test["billing_agreement_id"].unique()
    mask = [i not in user_embeddings_pooled_non_cold_start for i in users]
    missing_users = users[mask]
    df_user_cold_start = df_order_history_test[df_order_history_test["billing_agreement_id"].isin(missing_users)][
        ["billing_agreement_id", "concept_combination_list"]
    ].drop_duplicates(subset="billing_agreement_id")

    cold_start_user_embedings_dict_pooled = get_cold_start_users_embeddings(
        df_cold_start_user_preferences=df_user_cold_start,
        df_non_cold_start_order_history=df_order_history_train,
        id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
        top_n_per_concept=8,
        top_n_per_user=5,
        look_back_weeks=24,
        pooling_method="mean",
    )

    return cold_start_user_embedings_dict_pooled


def compute_precision(
    df_order_history_test: pd.DataFrame,
    df_recommendations: pd.DataFrame,
) -> pd.DataFrame:
    df_users_to_predict_agg = pd.DataFrame(
        df_order_history_test.groupby(["billing_agreement_id", "menu_year", "menu_week"])[
            "main_recipe_id"
        ].value_counts()
    ).reset_index()
    df_val = pd.DataFrame(
        df_users_to_predict_agg.groupby(["billing_agreement_id", "menu_year", "menu_week"])["main_recipe_id"].agg(list)
    ).reset_index()
    df_val = df_val.rename(columns={"main_recipe_id": "purchased_recipes"})

    df_precision_at_k = df_recommendations.merge(
        df_val, on=["menu_week", "menu_year", "billing_agreement_id"], how="inner"
    )
    df_precision_at_k["is_recommendation_purchased"] = df_precision_at_k.apply(
        lambda x: [i in (x["purchased_recipes"]) for i in x["top_n_recipe_ids"]], axis=1
    )
    df_precision_at_k["num_purchased_recommendations"] = df_precision_at_k["is_recommendation_purchased"].apply(
        lambda x: sum(x)
    )
    df_precision_at_k["num_purchased_recipes"] = df_precision_at_k["purchased_recipes"].apply(lambda x: len(x))

    return df_precision_at_k
