import logging

import pandas as pd
from reci_pick.postprocessing import (
    check_for_preference_violation,
    penalize_high_menu_occurance,
)
from reci_pick.predict.configs.predict_configs import CompanyPredictConfigs


def modify_scores_for_recommendations(
    df_scores: pd.DataFrame,
    df_taste_preferences: pd.DataFrame,
    company_configs: CompanyPredictConfigs,
    df_recipes: pd.DataFrame,
    df_menu_recipes: pd.DataFrame,
    df_menus_to_predict: pd.DataFrame,
) -> pd.DataFrame:
    logging.info("Modifying scores to penalize preference violations...")
    df_score_modified = check_for_preference_violation(
        score_df=df_scores,
        df_taste_preference=df_taste_preferences,
        df_recipes=df_recipes[
            ["main_recipe_id", "recipe_main_ingredient_name_english", "allergen_preference_id_list"]
        ].drop_duplicates(subset="main_recipe_id"),
        score_col="score_modified",
    )
    logging.info("Modifying scores to penalize high occurrence menu items...")
    df_score_modified = penalize_high_menu_occurance(
        df_menu_recipes=df_menu_recipes.copy(),
        df_menu_to_predict=df_menus_to_predict,
        df_scores=df_score_modified,
        alpha=company_configs.alpha,
        penalization_factor=company_configs.high_frequency_penalization_factor,
        score_col="score_modified",
    )
    return df_score_modified


def make_top_k_menu_recommendations(
    df_menu_scores: pd.DataFrame,
    score_col: str,
    top_k: int,
) -> pd.DataFrame:
    df_menu_scores = df_menu_scores.sort_values(
        by=["menu_yyyyww", "billing_agreement_id", score_col], ascending=[True, True, False]
    )
    df_top_n_recommendations = df_menu_scores.groupby(
        ["menu_year", "menu_week", "menu_yyyyww", "billing_agreement_id"]
    ).head(top_k)
    return df_top_n_recommendations
