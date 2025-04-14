import logging
from datetime import datetime
from typing import Literal

import mlflow
import numpy as np
import pytz
from constants.companies import get_company_by_code
from mlflow.models import ModelSignature, infer_signature
from mlflow.types.schema import Schema, TensorSpec
from pydantic import BaseModel
from reci_pick.preprocessing import preprocess_recipes_dataframe, split_train_test
from reci_pick.train.configs.train_configs import get_company_train_configs
from reci_pick.train.data import get_dataframes
from reci_pick.train.metrics import get_recommendation_precisions
from reci_pick.train.model import train_nn_model
from reci_pick.train.training_data import get_inputs_for_training, get_recipe_embeddings


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "test", "prod"]
    is_run_on_databricks: bool = True
    is_from_workflow: bool = False
    is_register_model: bool = False
    profile_name: str = "sylvia-liu"


def train_model(args: Args) -> None:
    company_code = args.company
    company_id = get_company_by_code(company_code).company_id

    company_configs = get_company_train_configs(company_code=company_code)
    start_yyyyww = company_configs.train_start_yyyyww

    logging.info("Downloading data...")
    df_recipes, df_menu_recipes, df_order_history = get_dataframes(
        company_id=company_id, start_yyyyww=start_yyyyww, env=args.env
    )
    logging.info("Preprocessing recipes...")
    df_recipes_processed, fitted_preprocessor = preprocess_recipes_dataframe(
        df_recipes=df_recipes.drop(columns=["allergen_id_list"]), company_configs=company_configs
    )

    logging.info("Creating recipe embeddings...")
    (
        id_to_recipe_embedding_lookup,
        _,
    ) = get_recipe_embeddings(
        df_recipes_processed=df_recipes_processed,
        recipe_numeric_features=company_configs.recipe_numeric_features,
    )
    df_order_history_train, df_order_history_test, split_yyyyww = split_train_test(
        df_order_history=df_order_history, num_prediction_weeks=company_configs.num_validation_weeks
    )
    logging.info(f"First validation week is {split_yyyyww}.")
    logging.info("Preparing input data for model...")
    user_embeddings_input, recipe_embeddings_input, df_train, user_embeddings_pooled_dict = get_inputs_for_training(
        df_order_history_train=df_order_history_train,
        df_menu_recipes=df_menu_recipes,
        id_to_embedding_lookup=id_to_recipe_embedding_lookup,
        pooling_method="mean",
        training_size=company_configs.training_size,
        is_pad_popular_recipes=True,
        min_recipes_per_user=5,
    )

    target = df_train["is_purchase"].values

    mlflow.set_experiment("/Shared/ml_experiments/reci-pick")
    timezone = pytz.timezone("UTC")
    timestamp_now = datetime.now(tz=timezone).strftime("%Y-%m-%d-%H:%M:%S")
    run_name = f"{args.company}_{timestamp_now}"

    with mlflow.start_run(run_name=run_name) as run:
        logging.info("Training model...")
        trained_model, last_loss, last_accuracy = train_nn_model(
            user_embeddings_input=user_embeddings_input, recipe_embeddings_input=recipe_embeddings_input, target=target
        )
        mlflow.log_metrics({"last_loss": last_loss, "last_accuracy": last_accuracy})
        df_precision_by_week, cold_start_precision, non_cold_start_precision = get_recommendation_precisions(
            df_order_history_train=df_order_history_train,
            df_order_history_test=df_order_history_test,
            user_embeddings_pooled_dict=user_embeddings_pooled_dict,
            id_to_recipe_embedding_lookup=id_to_recipe_embedding_lookup,
            df_menu_recipes=df_menu_recipes,
            trained_model=trained_model,
            num_test_users=company_configs.num_test_users,
        )
        mlflow.log_metrics(
            {
                "precision_at_10_cold_start": cold_start_precision,
                "precision_at_10_non_cold_start": non_cold_start_precision,
            }
        )
        for _, row in df_precision_by_week.iterrows():
            mlflow.log_metric(
                f"pecision_{row['menu_year'].astype(int)}_{row['menu_week'].astype(int)}",
                row["num_purchased_recommendations"],
            )
        input_schema = Schema(
            [
                TensorSpec(np.dtype(np.float32), (-1, user_embeddings_input.shape[1]), "user_profile"),
                TensorSpec(np.dtype(np.float32), (-1, recipe_embeddings_input.shape[1]), "recipe_profile"),
            ]
        )
        output_schema = Schema(
            [
                TensorSpec(
                    np.dtype(np.float32), (-1, 1), "predictions"
                ),  # Assuming the model outputs a single prediction per input
            ]
        )

        logging.info("Logging model...")
        signature_model = ModelSignature(inputs=input_schema, outputs=output_schema)
        mlflow.tensorflow.log_model(model=trained_model, artifact_path="model", signature=signature_model)

        logging.info("Logging preprocessor...")
        signature_preprocessor = infer_signature(
            model_input=df_recipes.head(1), model_output=df_recipes_processed.head(1)
        )
        mlflow.sklearn.log_model(fitted_preprocessor, artifact_path="preprocessor", signature=signature_preprocessor)
        training_params = company_configs.__dict__
        mlflow.log_params(training_params)
        logging.info("Registering model and preprocessor...")
        if args.is_register_model:
            run_uuid = run.info.run_id
            mlflow.set_registry_uri("databricks-uc")
            if args.is_from_workflow:
                registered_model_name = f"{args.env}.mloutputs.reci_pick_{company_code.lower()}"
                registered_model_name_preprocessor = (
                    f"{args.env}.mloutputs.reci_pick_preprocessor_{company_code.lower()}"
                )
            else:
                registered_model_name = f"{args.env}.mloutputs.reci_pick_local_{company_code.lower()}"
                registered_model_name_preprocessor = (
                    f"{args.env}.mloutputs.reci_pick_preprocessor_local_{company_code.lower()}"
                )
            mlflow.register_model(f"runs:/{run_uuid}/model", registered_model_name)
            mlflow.register_model(f"runs:/{run_uuid}/preprocessor", registered_model_name_preprocessor)
