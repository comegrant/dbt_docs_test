import logging
import uuid
from datetime import datetime
from typing import Literal

import mlflow
import numpy as np
import pandas as pd
from constants.companies import get_company_by_code
from pydantic import BaseModel
from pytz import timezone
from reci_pick.db import append_pandas_df_to_catalog
from reci_pick.postprocessing import map_new_recipes_with_old, modify_score_based_on_purchase_history
from reci_pick.predict.configs.predict_configs import get_company_predict_configs
from reci_pick.predict.data import get_dataframes
from reci_pick.predict.model import get_model_and_version
from reci_pick.predict.outputs import (
    prepare_concept_recommendations,
    prepare_concept_user_scores_for_output,
    prepare_meta_data_menus_predicted,
    prepare_outputs,
    prepare_recommendations_for_output,
)
from reci_pick.predict.predict_data import (
    divide_users_into_chunks,
    get_cold_start_users_embeddings,
    get_menu_to_predict,
    get_user_embeddings,
)
from reci_pick.predict.recommendations import make_top_k_menu_recommendations, modify_scores_for_recommendations
from reci_pick.preprocessing import preprocess_recipes_dataframe
from reci_pick.train.model import predict_recipe_scores
from reci_pick.train.training_data import get_recipe_embeddings

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    predict_date: str | None = None
    num_weeks: int = 4
    is_run_on_databricks: bool = True
    is_from_workflow: bool = False
    profile_name: str = "sylvia-liu"
    topk: int = 10


def predict_recipes(args: Args) -> None:
    company_code = args.company
    company_properties = get_company_by_code(company_code)
    company_id = company_properties.company_id
    company_predict_configs = get_company_predict_configs(company_code=company_code)

    logger.info("Getting dataframes...")
    df_recipes, df_menu_recipes, df_order_history, df_active_users, df_concept_preferences = get_dataframes(
        company_id=company_id, start_yyyyww=company_predict_configs.user_profile_start_yyyyww, env=args.env
    )

    logger.info("Preprocessing recipes...")
    preprocessor_uri = company_predict_configs.preprocessor_uri[args.env]
    recipe_preprocessor = mlflow.sklearn.load_model(preprocessor_uri)
    df_recipes_processed, _ = preprocess_recipes_dataframe(
        df_recipes=df_recipes.drop(columns=["allergen_id_list"]),
        company_configs=company_predict_configs,
        fitted_recipe_transformer=recipe_preprocessor,
    )
    logger.info("Getting recipe embeddings...")
    id_to_recipe_embedding_lookup, id_to_name_lookup = get_recipe_embeddings(
        df_recipes_processed=df_recipes_processed,
        recipe_numeric_features=company_predict_configs.recipe_numeric_features,
    )

    logger.info("Getting user embeddings...")
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
    yyyywws = menus_to_predict["menu_year"] * 100 + menus_to_predict["menu_week"]
    start_week = yyyywws.min()
    end_week = yyyywws.max()
    recipes_to_predict = menus_to_predict["main_recipe_id"].unique()
    n_recipes = len(recipes_to_predict)
    logger.info(f"Predicting for menu week {start_week} to {end_week}, {n_recipes} unique recipes...")
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
    trained_model, model_version = get_model_and_version(model_uri=model_uri)

    users_list = list(user_embedding_dict.keys())
    n_users = len(users_list)
    user_chunks = divide_users_into_chunks(user_id_list=users_list, num_chunks=company_predict_configs.num_user_chunks)
    logger.info(f"Total num users = {n_users}, divided into {company_predict_configs.num_user_chunks} chunks.")
    # df_topk_recommendations_ls = []
    timestamp_prediction = datetime.now(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    run_id = str(uuid.uuid4())
    logger.info(f"Start prediction at {timestamp_prediction}.")
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
            logger.info("Modifying scores to reward repeated dishes...")
            df_score_modified = modify_score_based_on_purchase_history(
                score_df_exploded=df_scores,
                df_order_history=df_order_history,
                bonus_factor=company_predict_configs.repeated_purchase_bonus_factor,
                is_map_similar_recipes=company_predict_configs.is_map_similar_recipes,
                df_similar_recipes=df_similar_recipes,
            )
            df_outputs = prepare_outputs(
                df_scores=df_score_modified,
                model_version=model_version,
                identifier_col="billing_agreement_id",
                score_col="score_modified",
                company_id=company_id,
                timestamp_prediction=timestamp_prediction,
                run_id=run_id,
            )
            append_pandas_df_to_catalog(df=df_outputs, table_name="mloutputs.reci_pick_scores", env=args.env)
            logger.info(f"Prediction scores written into DB for chunk {i} of users.")

            df_scores_modified = modify_scores_for_recommendations(
                df_scores=df_score_modified,
                df_taste_preferences=df_active_users,
                company_configs=company_predict_configs,
                df_recipes=df_recipes,
                df_menu_recipes=df_menu_recipes,
                df_menus_to_predict=menus_to_predict,
            )
            df_topk_recommendations = make_top_k_menu_recommendations(
                top_k=args.topk, df_menu_scores=df_scores_modified, score_col="score_modified"
            )
            df_top_k_outputs = prepare_recommendations_for_output(
                df_topk_recommendations=df_topk_recommendations,
                identifier_col="billing_agreement_id",
                score_col="score_modified",
                model_version=model_version,
                company_id=company_id,
                timestamp_prediction=timestamp_prediction,
                run_id=run_id,
            )
            append_pandas_df_to_catalog(
                df=df_top_k_outputs, table_name="mloutputs.reci_pick_recommendations", env=args.env
            )
            logger.info(f"Recommendations written into DB for chunk {i} of users.")
        except Exception as e:
            logger.error(f"Error predicting for {i}/{company_predict_configs.num_user_chunks} chunks of users: {e}")
            # Continue with next chunk of users to avoid breaking the loop
            continue
        logger.info("Successfully predicted scores and making recommendations for real users.")

    logger.info("Predicting default scores for concept combinations....")
    df_concept_users = pd.DataFrame(df_order_history["concept_combination_list"].drop_duplicates())
    df_concept_users["billing_agreement_id"] = df_concept_users.index

    concept_embeddings = get_cold_start_users_embeddings(
        df_non_cold_start_order_history=df_order_history,
        df_cold_start_user_preferences=df_concept_users,
        id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
        top_n_per_user=company_predict_configs.top_n_per_user,
        top_n_per_concept=company_predict_configs.top_n_per_concept,
        look_back_weeks=company_predict_configs.look_back_weeks,
        pooling_method=company_predict_configs.pooling_method,
    )
    df_scores_concept = predict_recipe_scores(
        recipe_ids_to_predict=recipes_to_predict,
        user_billing_agreements=df_concept_users["billing_agreement_id"],
        user_embeddings_pooled_dict=concept_embeddings,
        id_to_embedding_lookup=id_to_recipe_embedding_lookup,
        model=trained_model,
    )
    df_scores_concept_outputs = prepare_concept_user_scores_for_output(
        df_scores_concept=df_scores_concept,
        df_concept_users=df_concept_users,
        df_concept_preferences=df_concept_preferences,
        model_version=model_version,
        timestamp_prediction=timestamp_prediction,
        company_id=company_id,
        run_id=run_id,
    )
    append_pandas_df_to_catalog(
        df=df_scores_concept_outputs, table_name="mloutputs.reci_pick_scores_concept_default", env=args.env
    )
    logger.info("Default prediction scores written into DB for concept combinations.")
    logger.info("Making default recommendations for concept combinations...")
    df_menu_scores_concept = df_scores_concept.merge(menus_to_predict)
    df_recs_concept = make_top_k_menu_recommendations(
        df_menu_scores=df_menu_scores_concept, top_k=args.topk, score_col="score"
    )
    df_recs_concept_outputs = prepare_concept_recommendations(
        df_recs_concept=df_recs_concept,
        df_concept_users=df_concept_users,
        df_concept_preferences=df_concept_preferences,
        model_version=model_version,
        timestamp_prediction=timestamp_prediction,
        company_id=company_id,
        run_id=run_id,
    )
    append_pandas_df_to_catalog(
        df=df_recs_concept_outputs, table_name="mloutputs.reci_pick_recommendations_concept_default", env=args.env
    )
    logger.info("Default recommendations written into DB for concept combinations.")
    logger.info("Creating metadata....")
    df_menus_predicted = prepare_meta_data_menus_predicted(
        df_menus_predicted=menus_to_predict,
        run_id=run_id,
        timestamp_prediction=timestamp_prediction,
        company_id=company_id,
        num_users=np.array([len(i) for i in user_chunks]).sum(),
    )
    append_pandas_df_to_catalog(
        df=df_menus_predicted, table_name="mloutputs.reci_pick_scores_metadata_menus_predicted", env=args.env
    )
    logger.info("Meta data updated!")
    logger.info("Run successful!")
