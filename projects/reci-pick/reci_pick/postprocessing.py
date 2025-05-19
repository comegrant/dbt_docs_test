import numpy as np
import pandas as pd

from reci_pick.helpers import get_date_from_year_week, has_two_columns_intersection
from reci_pick.train.baseline import make_top_n_recommendations


def map_new_recipes_with_old(
    df_menu_recipes: pd.DataFrame,
    df_menus_to_predict: pd.DataFrame,
    id_to_embedding_lookup: dict,
    id_to_name_lookup: dict,
    similarity_threshold: float | None = 0.91,
) -> pd.DataFrame:
    df_recipe_last_occurance = get_recipe_last_appearance(df_menu_recipes=df_menu_recipes.copy())
    menu_recipes_last_occurance = df_recipe_last_occurance.merge(df_menus_to_predict, how="inner")
    df_new_recipes = menu_recipes_last_occurance[(menu_recipes_last_occurance["recipe_last_appeared_yyyyww"].isnull())]
    new_recipe_ids = df_new_recipes["main_recipe_id"].unique()
    new_recipe_embeddings = {}
    old_recipe_embeddings = {}
    for main_recipe_id, embedding_vector in id_to_embedding_lookup.items():
        if main_recipe_id in new_recipe_ids:
            new_recipe_embeddings[main_recipe_id] = embedding_vector.reshape(1, -1)
        else:
            old_recipe_embeddings[main_recipe_id] = embedding_vector
    most_similar_recipes = make_top_n_recommendations(
        user_positive_embeddings=new_recipe_embeddings,
        target_product_embeddings=old_recipe_embeddings,
        id_to_name_lookup=id_to_name_lookup,
        top_n=1,
    )
    df_most_similar_recipes = pd.DataFrame(most_similar_recipes).transpose().reset_index()
    df_most_similar_recipes = df_most_similar_recipes.rename(
        columns={
            "index": "main_recipe_id",
            "top_n_recipe_ids": "most_similar_recipe",
            "top_n_scores": "similarity_score",
        }
    )
    df_most_similar_recipes = df_most_similar_recipes.explode(["most_similar_recipe", "similarity_score"])
    df_most_similar_recipes = df_most_similar_recipes[
        df_most_similar_recipes["similarity_score"] > similarity_threshold
    ][["main_recipe_id", "most_similar_recipe", "similarity_score"]]
    return df_most_similar_recipes


def compute_historical_purchase(df_order_history: pd.DataFrame) -> pd.DataFrame:
    df_historical_purchase = pd.DataFrame(
        df_order_history.groupby(["billing_agreement_id"])["main_recipe_id"].value_counts()
    ).reset_index()

    df_historical_purchase = df_historical_purchase.rename(columns={"count": "num_purchases"})
    return df_historical_purchase


def modify_score_based_on_purchase_history(
    score_df_exploded: pd.DataFrame,
    df_order_history: pd.DataFrame,
    bonus_factor: float = 0.25,
    is_map_similar_recipes: bool | None = False,
    df_similar_recipes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if is_map_similar_recipes:
        score_df_exploded = score_df_exploded.merge(df_similar_recipes, how="left")

        score_df_exploded.loc[score_df_exploded["most_similar_recipe"].isnull(), "most_similar_recipe"] = (
            score_df_exploded[score_df_exploded["most_similar_recipe"].isnull()]["main_recipe_id"]
        )

        score_df_exploded["similarity_score"] = score_df_exploded["similarity_score"].fillna(1.0)
        score_df_exploded = score_df_exploded.rename(
            columns={
                "main_recipe_id": "main_recipe_id_original",
                "most_similar_recipe": "main_recipe_id",
            }
        )
    else:
        score_df_exploded["similarity_score"] = 1.0

    df_historical_purchase = compute_historical_purchase(df_order_history=df_order_history)
    score_df_exploded_modified = score_df_exploded.merge(
        df_historical_purchase, on=["billing_agreement_id", "main_recipe_id"], how="left"
    )
    score_df_exploded_modified["num_purchases"] = score_df_exploded_modified["num_purchases"].fillna(0)
    score_df_exploded_modified["score_modified"] = (
        score_df_exploded_modified["similarity_score"] * score_df_exploded_modified["num_purchases"] * bonus_factor
        + score_df_exploded_modified["score"]
    )
    score_df_exploded_modified = score_df_exploded_modified.sort_values(by="score_modified", ascending=False)
    if is_map_similar_recipes:
        score_df_exploded_modified = score_df_exploded_modified.drop(columns=["main_recipe_id"])
        score_df_exploded_modified = score_df_exploded_modified.rename(
            columns={"main_recipe_id_original": "main_recipe_id"}
        )

    return score_df_exploded_modified


def check_for_preference_violation(
    score_df: pd.DataFrame,
    df_taste_preference: pd.DataFrame,
    df_recipes: pd.DataFrame,
    score_col: str = "score_modified",
    allergen_penalization_factor: float = 0.0,
    preference_penalization_factor: float = 0.1,
) -> pd.DataFrame:
    # Check for allergens
    score_df = score_df.merge(
        df_taste_preference[["billing_agreement_id", "taste_preference_combinations", "allergen_preference_id_list"]]
    )
    score_df = score_df.merge(
        df_recipes[["main_recipe_id", "recipe_main_ingredient_name_english", "allergen_preference_id_list"]].rename(
            columns={"allergen_preference_id_list": "allergen_preference_id_recipe"}
        )
    )
    score_df["taste_preference_combinations_list"] = score_df["taste_preference_combinations"].str.split(", ")
    # Check for allergen violations using sets
    score_df["is_violate_allergens"] = score_df.apply(
        has_two_columns_intersection, col1="allergen_preference_id_list", col2="allergen_preference_id_recipe", axis=1
    )
    score_df["recipe_main_ingredient_name_english"] = score_df["recipe_main_ingredient_name_english"].fillna("Unknown")
    # Check for main ingredient violations
    score_df["is_main_ingredient_violation"] = score_df.apply(
        lambda x: x["recipe_main_ingredient_name_english"] in x["taste_preference_combinations"], axis=1
    )
    score_df.loc[score_df["is_violate_allergens"], score_col] = allergen_penalization_factor
    score_df.loc[score_df["is_main_ingredient_violation"], score_col] = (
        preference_penalization_factor * score_df[score_df["is_main_ingredient_violation"]][score_col]
    )
    return score_df


def get_recipe_last_appearance(df_menu_recipes: pd.DataFrame) -> pd.DataFrame:
    df_menu_recipes["recipe_last_appeared_yyyyww"] = df_menu_recipes.groupby(["main_recipe_id"])["menu_yyyyww"].shift(1)
    dummy_yyyyww = 190001
    df_menu_recipes["recipe_last_appeared_yyyyww"] = df_menu_recipes["recipe_last_appeared_yyyyww"].fillna(dummy_yyyyww)
    df_menu_recipes["recipe_last_appeared_yyyyww"] = df_menu_recipes["recipe_last_appeared_yyyyww"].astype(int)
    df_menu_recipes["recipe_last_appeared_year"] = df_menu_recipes["recipe_last_appeared_yyyyww"] // 100
    df_menu_recipes["recipe_last_appeared_week"] = df_menu_recipes["recipe_last_appeared_yyyyww"] % 100

    df_menu_recipes["last_appeared_monday"] = pd.to_datetime(
        df_menu_recipes.apply(
            lambda x: get_date_from_year_week(
                year=x["recipe_last_appeared_year"], week_number=x["recipe_last_appeared_week"], weekday=1
            ),
            axis=1,
        )
    )

    df_menu_recipes["menu_yyyyww_monday"] = pd.to_datetime(
        df_menu_recipes.apply(
            lambda x: get_date_from_year_week(year=x["menu_year"], week_number=x["menu_week"], weekday=1), axis=1
        )
    )

    df_menu_recipes["num_weeks_since_last_appeared"] = (
        df_menu_recipes["menu_yyyyww_monday"] - df_menu_recipes["last_appeared_monday"]
    ).dt.days / 7

    df_menu_recipes.loc[
        df_menu_recipes["recipe_last_appeared_yyyyww"] == dummy_yyyyww, "num_weeks_since_last_appeared"
    ] = None
    df_menu_recipes.loc[
        df_menu_recipes["recipe_last_appeared_yyyyww"] == dummy_yyyyww, "recipe_last_appeared_yyyyww"
    ] = None

    df_menu_recipes = df_menu_recipes.drop(
        columns=[
            "recipe_last_appeared_year",
            "recipe_last_appeared_week",
            "last_appeared_monday",
            "menu_yyyyww_monday",
        ]
    )
    return df_menu_recipes


def penalize_high_menu_occurance(
    df_menu_recipes: pd.DataFrame,
    df_menu_to_predict: pd.DataFrame,
    df_scores: pd.DataFrame,
    alpha: float | None = -1.0,
    penalization_factor: float | None = 0.1,
    score_col: str | None = "score_modified",
    random_seed: int | None = 42,
) -> pd.DataFrame:
    """This function generates a probabilistic penalization for recipes
    that occur very recently on the menu.

    It first calculate n = number of weeks the recipe last appears, and
    p(penalization) = exp(alpha * n), i.e.,
        for a recipe which appears on the menu n=1 week ago, alpha = -1 implies that
        there is exp(-1), which is about 30% chance this recipe will be penalized.
    Set alpha to be closer to 0 if you want to increase the chance of penalization.

    The penalization is defined by multiplying the score with the penalization factor,
        default at 0.1

    """
    df_menu_recipes_penalization = get_recipe_last_appearance(df_menu_recipes=df_menu_recipes)
    df_menu_recipes_penalization = df_menu_recipes_penalization.merge(df_menu_to_predict, how="inner")
    # Set the default to be 0
    df_menu_recipes_penalization["penalization_probability"] = 0.0

    df_menu_recipes_penalization.loc[
        ~df_menu_recipes_penalization["num_weeks_since_last_appeared"].isnull(), "penalization_probability"
    ] = np.exp(
        df_menu_recipes_penalization[~df_menu_recipes_penalization["num_weeks_since_last_appeared"].isnull()][
            "num_weeks_since_last_appeared"
        ]
        * (alpha)
    )

    df_menu_scores = df_menu_recipes_penalization.merge(df_scores, how="left")
    # Add the random number by a tiny small such that if alpha = 0 (no penalization),
    # the condition will be satisfied
    np.random.seed(random_seed)
    df_menu_scores["rand_num"] = np.random.rand(df_menu_scores.shape[0])
    df_menu_scores["is_penalize_recommendation"] = (
        df_menu_scores["rand_num"] < df_menu_scores["penalization_probability"]
    )
    df_menu_scores.loc[df_menu_scores["is_penalize_recommendation"], score_col] = (
        penalization_factor * df_menu_scores[df_menu_scores["is_penalize_recommendation"]][score_col]
    )

    return df_menu_scores.drop(columns=["rand_num", "num_weeks_since_last_appeared"])
