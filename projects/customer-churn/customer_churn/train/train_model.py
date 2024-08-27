import gc
import logging

import mlflow
import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.training_set import TrainingSet
from pycaret.classification import setup
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from customer_churn.models.mlflow import MlflowConfig
from customer_churn.models.train import TrainConfig
from customer_churn.paths import PROJECT_DIR
from customer_churn.train.make_dataset import create_train_val_split

logger = logging.getLogger(__name__)


def train_ensemble_model(
    df_training_set: TrainingSet,
    company_code: str,
    train_config: TrainConfig,
    mlflow_config: MlflowConfig,
    env: str,
) -> None:
    logger.info("Training model with MLflow")
    model_config = train_config.ml_model_config
    mlflow.set_tracking_uri(mlflow_config.mlflow_tracking_uri)
    mlflow.set_experiment(mlflow_config.experiment_tracking_dir)

    df_train, df_holdout = create_train_val_split(
        training_set=df_training_set,
        train_start_yyyyww=train_config.train_start_yyyyww,
        num_holdout_weeks=train_config.num_holdout_weeks,
    )

    with mlflow.start_run() as mlflow_run:
        logger.info("Starting model training with Pycaret")
        logger.info("Columns in training set: " + str(df_train.columns.tolist()))
        logger.info("Nans in training set: " + str(df_train.isna().sum()))
        logger.info(df_train.iloc[1])

        # Set up experiment
        experiment_classification = setup(
            data=df_train,
            target=train_config.target_col,
            test_data=df_holdout,
            fold_strategy=model_config.fold_strategy,
            data_split_shuffle=model_config.data_split_shuffle,
            memory=False,
            html=False,
        )

        # Train model
        logger.info(model_config)
        best_models = experiment_classification.compare_models(
            n_select=model_config.n_select,
            include=model_config.ml_model_list,
        )
        # To be logged
        df_train_info = experiment_classification.pull()

        top_models_blend = experiment_classification.blend_models(estimator_list=best_models)

        final_model = experiment_classification.finalize_model(top_models_blend)

        run_id = mlflow_run.info.run_uuid
        logger.info(f"Resource ID (Run ID): {run_id}")
        logger.info(f"Experiment ID: {mlflow_run.info.experiment_id}")

        df_train_artifact_path = f"{PROJECT_DIR}/df_train.csv"
        df_train.to_csv(df_train_artifact_path, index=False)
        df_train_info_artifact_path = f"{PROJECT_DIR}/training_info.csv"
        df_train_info.to_csv(df_train_info_artifact_path, index=False)

        del df_train
        del top_models_blend
        gc.collect()

        log_metrics(
            experiment_reg=experiment_classification,
            df_holdout=df_holdout,
            model=final_model,
            target_col=train_config.target_col,
        )

        mlflow.log_artifact(
            df_train_artifact_path,
            artifact_path="training_data",
        )
        mlflow.log_artifact(
            df_train_info_artifact_path,
            artifact_path="training_info",
        )

        save_model_databricks(
            env=env,
            mlflow_config=mlflow_config,
            training_set=df_training_set,
            model=final_model,
            company_code=company_code,
        )


def save_model_databricks(
    env: str,
    mlflow_config: MlflowConfig,
    training_set: TrainingSet,
    model: object,
    company_code: str,
) -> None:
    fe = FeatureEngineeringClient()

    mlmodel_container_name = mlflow_config.mlmodel_container_name
    model_name = mlflow_config.registered_model_name

    registered_model_name = f"{env}.{mlmodel_container_name}.{model_name}_{company_code}"

    fe.log_model(
        model=model,
        artifact_path=f"{mlflow_config.artifact_path}_{company_code}",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=registered_model_name,
    )
    mlflow.set_tag("env", env)
    mlflow.set_tag("registered_model_name", registered_model_name)
    mlflow.set_tag("company", company_code)


def log_metrics(experiment_reg: object, df_holdout: pd.DataFrame, model: object, target_col: str) -> None:
    actual = df_holdout.pop(target_col).to_list()
    prediction = experiment_reg.predict_model(model).pop("prediction_label").to_list()

    metric_dict = {
        "accuracy": accuracy_score(actual, prediction),
        "precision": precision_score(actual, prediction),
        "recall": recall_score(actual, prediction),
        "f1": f1_score(actual, prediction),
    }

    mlflow.log_metric("holdout_accuracy", metric_dict["accuracy"])
    mlflow.log_metric("holdout_precision", metric_dict["precision"])
    mlflow.log_metric("holdout_recall", metric_dict["recall"])
    mlflow.log_metric("holdout_f1", metric_dict["f1"])

    logger.info(metric_dict)
