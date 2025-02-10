import logging

import mlflow
import mlflow.sklearn
import optuna
import pandas as pd
from attribute_scoring.common import Args
from attribute_scoring.train.configs import DataConfig, ModelConfig
from attribute_scoring.train.model import model
from attribute_scoring.train.preprocessing import prepare_training_data
from attribute_scoring.train.train import train_model
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient

DATA_CONFIG = DataConfig()
MODEL_CONFIG = ModelConfig()


def tune_pipeline(args: Args, fe: FeatureEngineeringClient, spark: DatabricksSession, n_trials: int) -> None:
    """Executes the tuning pipeline for a given company and target.

    This functions iterates over different classifiers and does hyperparameter tuning
    for each classifier. The best model for each classifier is stored as the parent run,
    and each trial is stored as a child run with MLFlow.

    Args:
        args (Args): Configuration arguments.
        fe (FeatureEngineeringClient): A feature engineering client.
        spark (DatabricksSession): A Spark session.
    """
    logging.info(f"\nStarting tuning pipeline for {args.company}: {args.target}\n")

    data, target_encoded, _ = prepare_training_data(args=args, fe=fe, spark=spark)

    classifiers = ["xgboost", "random_forest", "catboost"]

    logging.info("Starting model tuning...")
    for classifier in classifiers:
        logging.info(f"\nTraining {classifier}...\n")
        with mlflow.start_run(run_name=f"{args.company}_{classifier}_{args.target}"):
            mlflow.log_param("classifier", classifier)
            mlflow.log_param("target", args.target)
            mlflow.set_tags({"company": args.company, "target": args.target})

            study = optuna.create_study(direction="maximize")
            study.optimize(
                lambda trial, classifier=classifier: objective(
                    trial=trial,
                    args=args,
                    classifier_name=classifier,
                    data=data,
                    target=target_encoded,
                ),
                n_trials=n_trials,
            )
            best_trial = study.best_trial
            if best_trial.value is None:
                raise ValueError("Optimization failed: best trial value is None")
            mlflow.log_params({f"best_{key}": value for key, value in best_trial.params.items()})
            mlflow.log_metric(f"best_{MODEL_CONFIG.evaluation_metric}", best_trial.value)

            model_pipeline = model(args, best_trial)
            mlflow.sklearn.log_model(model_pipeline, artifact_path="model")

    logging.info("Finished tuning.")


def objective(
    trial: optuna.Trial,
    args: Args,
    classifier_name: str,
    data: pd.DataFrame,
    target: pd.Series,
) -> float:
    """Defines the objective function for hyperparameter optimization."""
    if classifier_name == "xgboost":
        param = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 200),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "gamma": trial.suggest_float("gamma", 0, 5),
            "random_state": 42,
        }
        classifier = MODEL_CONFIG.get_xgboost_classifier(**param)

    elif classifier_name == "random_forest":
        param = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 300),
            "max_depth": trial.suggest_int("max_depth", 3, 20),
            "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
            "random_state": 42,
        }
        classifier = MODEL_CONFIG.get_random_forest_classifier(**param)

    elif classifier_name == "catboost":
        param = {
            "iterations": trial.suggest_int("iterations", 50, 300),
            "depth": trial.suggest_int("depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-8, 10.0, log=True),
            "random_strength": trial.suggest_float("random_strength", 1e-8, 10.0, log=True),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
            "verbose": False,
            "random_state": 42,
        }
        classifier = MODEL_CONFIG.get_catboost_classifier(**param)
    else:
        raise ValueError(f"Unnsupported classifier: {classifier_name}")

    model_pipeline = model(args, classifier)

    with mlflow.start_run(nested=True):
        _, eval_metric = train_model(
            args=args,
            data=data,
            target_encoded=target,
            model_pipeline=model_pipeline,
            tune_model=True,
        )

    return eval_metric
