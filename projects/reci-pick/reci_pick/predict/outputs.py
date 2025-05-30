from datetime import datetime

import pandas as pd


def prepare_outputs(
    df_scores: pd.DataFrame,
    model_version: str,
    identifier_col: str,
    score_col: str,
    timestamp_prediction: datetime,
    company_id: str,
    run_id: str,
) -> pd.DataFrame:
    df_scores_output = df_scores[
        [
            identifier_col,  # "billing_agreement_id",
            "main_recipe_id",
            score_col,
        ]
    ].rename(columns={score_col: "score"})
    df_scores_output["model_version"] = model_version
    df_scores_output["created_at"] = timestamp_prediction
    df_scores_output["run_id"] = run_id
    df_scores_output["company_id"] = company_id
    columns_rearranged = [
        "company_id",
        identifier_col,
        "main_recipe_id",
        "score",
        "model_version",
        "run_id",
        "created_at",
    ]
    df_scores_output = df_scores_output[columns_rearranged]
    return df_scores_output


def prepare_recommendations_for_output(
    df_topk_recommendations: pd.DataFrame,
    identifier_col: str,  # billing_agreement_id or concept_combinations
    score_col: str,
    model_version: str,
    timestamp_prediction: datetime,
    company_id: str,
    run_id: str,
) -> pd.DataFrame:
    df_rec_outputs = (
        df_topk_recommendations.groupby([identifier_col, "menu_year", "menu_week"])[["main_recipe_id", score_col]]
        .agg(list)
        .reset_index()
    )
    df_rec_outputs = df_rec_outputs.rename(
        columns={
            score_col: "scores",
            "main_recipe_id": "main_recipe_ids",
        }
    )
    df_rec_outputs["model_version"] = model_version
    df_rec_outputs["created_at"] = timestamp_prediction
    df_rec_outputs["run_id"] = run_id
    df_rec_outputs["company_id"] = company_id
    columns_rearranged = [
        "company_id",
        identifier_col,
        "menu_year",
        "menu_week",
        "main_recipe_ids",
        "scores",
        "model_version",
        "run_id",
        "created_at",
    ]
    return df_rec_outputs[columns_rearranged]


def prepare_concept_user_scores_for_output(
    df_scores_concept: pd.DataFrame,
    df_concept_users: pd.DataFrame,
    df_concept_preferences: pd.DataFrame,
    model_version: str,
    timestamp_prediction: datetime,
    company_id: str,
    run_id: str,
) -> pd.DataFrame:
    df_scores_concept = df_scores_concept.merge(df_concept_users)
    df_scores_concept["concept_name_combinations"] = df_scores_concept["concept_combination_list"].apply(
        lambda x: ", ".join(x)
    )
    df_scores_concept = df_scores_concept.merge(
        df_concept_preferences[["concept_name_combinations", "concept_preference_id_list"]].drop_duplicates(
            subset=["concept_name_combinations"]
        )
    )
    df_scores_concept["concept_id_combinations"] = df_scores_concept["concept_preference_id_list"].apply(
        lambda x: ", ".join(x)
    )
    df_scores_concept = df_scores_concept[
        ["concept_id_combinations", "concept_name_combinations", "main_recipe_id", "score"]
    ]
    df_scores_concept_outputs = prepare_outputs(
        df_scores=df_scores_concept,
        model_version=model_version,
        identifier_col="concept_id_combinations",
        score_col="score",
        timestamp_prediction=timestamp_prediction,
        company_id=company_id,
        run_id=run_id,
    )
    return df_scores_concept_outputs


def prepare_concept_recommendations(
    df_recs_concept: pd.DataFrame,
    df_concept_users: pd.DataFrame,
    df_concept_preferences: pd.DataFrame,
    model_version: str,
    timestamp_prediction: datetime,
    company_id: str,
    run_id: str,
) -> pd.DataFrame:
    df_recs_concept = df_recs_concept.merge(df_concept_users)
    df_recs_concept["concept_name_combinations"] = df_recs_concept["concept_combination_list"].apply(
        lambda x: ", ".join(x)
    )
    df_recs_concept = df_recs_concept.merge(
        df_concept_preferences[["concept_name_combinations", "concept_preference_id_list"]].drop_duplicates(
            subset=["concept_name_combinations"]
        )
    )
    df_recs_concept["concept_id_combinations"] = df_recs_concept["concept_preference_id_list"].apply(
        lambda x: ", ".join(x)
    )
    df_recs_concept_output = prepare_recommendations_for_output(
        df_topk_recommendations=df_recs_concept,
        identifier_col="concept_id_combinations",
        score_col="score",
        model_version=model_version,
        timestamp_prediction=timestamp_prediction,
        company_id=company_id,
        run_id=run_id,
    )

    return df_recs_concept_output


def prepare_meta_data_menus_predicted(
    df_menus_predicted: pd.DataFrame, company_id: str, run_id: str, timestamp_prediction: datetime, num_users: int
) -> pd.DataFrame:
    df_menus_predicted = df_menus_predicted[["menu_year", "menu_week"]].drop_duplicates()
    df_menus_predicted["run_id"] = run_id
    df_menus_predicted["company_id"] = company_id
    df_menus_predicted["created_at"] = timestamp_prediction
    df_menus_predicted["num_users_predicted"] = num_users
    return df_menus_predicted
