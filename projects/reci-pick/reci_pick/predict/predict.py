import logging
from typing import Literal

import mlflow
from constants.companies import get_company_by_code
from pydantic import BaseModel
from reci_pick.postprocessing import map_new_recipes_with_old
from reci_pick.predict.configs.predict_configs import get_company_predict_configs
from reci_pick.predict.data import get_dataframes
from reci_pick.predict.predict_data import (
    divide_users_into_chunks,
    get_menu_to_predict,
    get_user_embeddings,
)
from reci_pick.predict.recommendations import make_top_k_menu_recommendations, modify_scores_for_recommendations
from reci_pick.preprocessing import preprocess_recipes_dataframe
from reci_pick.train.model import predict_recipe_scores
from reci_pick.train.training_data import get_recipe_embeddings


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    predict_date: str | None = None
    num_weeks: int = 4
    is_run_on_databricks: bool = True
    is_from_workflow: bool = False
    profile_name: str = "sylvia-liu"
    topk: int = 10


def predict_recommendations(args: Args) -> None:
    company_code = args.company
    company_properties = get_company_by_code(company_code)
    company_id = company_properties.company_id
    company_predict_configs = get_company_predict_configs(company_code=company_code)
    df_recipes, df_menu_recipes, df_order_history, df_active_users = get_dataframes(
        company_id=company_id, start_yyyyww=company_predict_configs.user_profile_start_yyyyww, env=args.env
    )
    preprocessor_uri = company_predict_configs.preprocessor_uri[args.env]
    recipe_preprocessor = mlflow.sklearn.load_model(preprocessor_uri)
    df_recipes_processed, _ = preprocess_recipes_dataframe(
        df_recipes=df_recipes.drop(columns=["allergen_id_list"]),
        company_configs=company_predict_configs,
        fitted_recipe_transformer=recipe_preprocessor,
    )
    id_to_recipe_embedding_lookup, id_to_name_lookup = get_recipe_embeddings(
        df_recipes_processed=df_recipes_processed,
        recipe_numeric_features=company_predict_configs.recipe_numeric_features,
    )

    user_embedding_dict = get_user_embeddings(
        df_order_history=df_order_history,
        df_user_preferences=df_active_users,
        id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
        top_n_per_concept=company_predict_configs.top_n_per_concept,
        top_n_per_user=company_predict_configs.top_n_per_user,
        look_back_weeks=company_predict_configs.look_back_weeks,
        pooling_method=company_predict_configs.pooling_method,
    )

    menus_to_predict = get_menu_to_predict(
        df_menu_recipes=df_menu_recipes,
        prediction_date=args.predict_date,
        num_weeks=args.num_weeks,
        cut_off_day=company_properties.cut_off_week_day,
    )
    recipes_to_predict = menus_to_predict["main_recipe_id"].unique()
    if company_predict_configs.is_map_similar_recipes:
        df_similar_recipes = map_new_recipes_with_old(
            df_menu_recipes=df_menu_recipes,
            df_menus_to_predict=menus_to_predict,
            id_to_embedding_lookup=id_to_recipe_embedding_lookup,
            id_to_name_lookup=id_to_name_lookup,
            similarity_threshold=company_predict_configs.similarity_threshold,
        )
    else:
        df_similar_recipes = None

    model_uri = company_predict_configs.model_uri[args.env]
    trained_model = mlflow.tensorflow.load_model(model_uri)

    users = list(user_embedding_dict.keys())

    user_chunks = divide_users_into_chunks(user_id_list=users, num_chunks=company_predict_configs.num_user_chunks)

    df_topk_recommendations_ls = []
    for i, users in enumerate(user_chunks):
        try:
            logging.info(f"Predicting for {i}/{company_predict_configs.num_user_chunks} chunks of users...")
            df_scores = predict_recipe_scores(
                recipe_ids_to_predict=recipes_to_predict,
                user_billing_agreements=users,
                user_embeddings_pooled_dict=user_embedding_dict,
                id_to_embedding_lookup=id_to_recipe_embedding_lookup,
                model=trained_model,
            )

            df_scores_modified = modify_scores_for_recommendations(
                df_scores=df_scores,
                df_order_history=df_order_history,
                df_similar_recipes=df_similar_recipes,
                df_taste_preferences=df_active_users,
                company_configs=company_predict_configs,
                df_recipes=df_recipes,
                df_menu_recipes=df_menu_recipes,
                df_menus_to_predict=df_menu_recipes,
            )
            df_topk_recommendations = make_top_k_menu_recommendations(
                top_k=args.topk, df_menu_scores=df_scores_modified
            )
            df_topk_recommendations_ls.append(df_topk_recommendations)
        except Exception as e:
            logging.error(f"Error predicting for {i}/{company_predict_configs.num_user_chunks} chunks of users: {e}")
            # Continue with next chunk of users to avoid breaking the loop
            continue
    return df_topk_recommendations_ls
