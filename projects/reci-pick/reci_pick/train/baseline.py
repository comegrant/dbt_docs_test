import numpy as np
import pandas as pd
from reci_pick.helpers import get_dict_values_as_array
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm


def make_user_embedding_profile(df_purchase_history: pd.DataFrame, id_to_embedding_lookup: dict) -> dict:
    embeddings_dict = {}

    df_order_history_grouped_by_user = pd.DataFrame(
        df_purchase_history.groupby("billing_agreement_id")["main_recipe_id"].value_counts()
    ).reset_index()
    agreement_ids = df_order_history_grouped_by_user["billing_agreement_id"].unique()
    for agreement_id in tqdm(agreement_ids):
        df_one_user = df_order_history_grouped_by_user[
            df_order_history_grouped_by_user["billing_agreement_id"] == agreement_id
        ]
        embeddings = get_dict_values_as_array(
            look_up_dict=id_to_embedding_lookup, key_list=df_one_user["main_recipe_id"].values
        )
        embeddings_dict[agreement_id] = embeddings
    return embeddings_dict


def make_top_n_recommendations(
    user_positive_embeddings: dict, target_product_embeddings: dict, id_to_name_lookup: dict, top_n: int | None = 8
) -> dict:
    top_n_recommendations = {}
    for an_agreement in tqdm(user_positive_embeddings.keys()):
        one_user_recommendation = {}
        user_embedding = user_positive_embeddings.get(an_agreement)

        main_recipe_ids = list(target_product_embeddings.keys())
        target_product_embedding_array = np.array(list(target_product_embeddings.values()))

        cosine_similarities = cosine_similarity(user_embedding, target_product_embedding_array)
        similarity_score_pooled = np.max(cosine_similarities, axis=0)

        top_n_index = np.argsort(similarity_score_pooled)[::-1][:top_n]
        top_n_recipes = np.array(main_recipe_ids)[top_n_index]
        top_n_scores = similarity_score_pooled[top_n_index]
        top_n_recipe_names = get_dict_values_as_array(look_up_dict=id_to_name_lookup, key_list=top_n_recipes)
        one_user_recommendation = {
            "top_n_recipe_ids": top_n_recipes,
            "top_n_recipe_names": top_n_recipe_names,
            "top_n_scores": top_n_scores,
        }
        top_n_recommendations[an_agreement] = one_user_recommendation
    return top_n_recommendations


def make_menu_recommendations(
    df_menu_to_predict: pd.DataFrame,
    user_positive_embeddings: dict,
    id_to_name_lookup: dict,
    id_to_embedding_lookup: dict,
    top_n: int | None = 8,
) -> pd.DataFrame:
    df_recommendations_list = []
    for i in df_menu_to_predict.itertuples():
        menu_embeddings = get_dict_values_as_array(look_up_dict=id_to_embedding_lookup, key_list=i.main_recipe_id)
        target_product_embeddings = dict(zip(i.main_recipe_id, menu_embeddings))
        top_n_recommendations = make_top_n_recommendations(
            user_positive_embeddings=user_positive_embeddings,
            target_product_embeddings=target_product_embeddings,
            id_to_name_lookup=id_to_name_lookup,
            top_n=top_n,
        )
        df_recommendations = pd.DataFrame(top_n_recommendations).T
        df_recommendations = df_recommendations.reset_index()
        df_recommendations = df_recommendations.rename(columns={"index": "billing_agreement_id"})
        df_recommendations["menu_year"] = i.menu_year
        df_recommendations["menu_week"] = i.menu_week
        df_recommendations_list.append(df_recommendations)

    df_final = pd.concat(df_recommendations_list)
    return df_final


def get_menus_to_predict(
    df_menu_recipes: pd.DataFrame,
    df_order_history_test: pd.DataFrame,
) -> pd.DataFrame:
    df_menu_recipes["menu_yyyyww"] = df_menu_recipes["menu_year"].astype(int) * 100 + df_menu_recipes["menu_week"]
    df_menu_targets = df_menu_recipes[df_menu_recipes["menu_yyyyww"].isin(df_order_history_test["menu_yyyyww"])]

    df_menu_to_predict = pd.DataFrame(
        df_menu_targets.groupby(["menu_year", "menu_week"])["main_recipe_id"].agg(list)
    ).reset_index()

    return df_menu_to_predict
