import logging

import mlflow
import optuna
import pandas as pd
from attribute_scoring.common import ArgsTrain
from attribute_scoring.train.configs import ModelConfig, DataConfig
from attribute_scoring.train.model import model
from attribute_scoring.train.train import train_model

from attribute_scoring.train.preprocessing import create_training_data
from attribute_scoring.train.data import get_training_data

from model_registry import databricks_model_registry, ModelRegistryBuilder


MODEL_CONFIG = ModelConfig()
DATA_CONFIG = DataConfig()


def tune_pipeline(
    args: ArgsTrain,
    n_trials: int,
    registry: ModelRegistryBuilder = databricks_model_registry(),
) -> None:
    logging.info(f"\nStarting tuning pipeline for {args.company}: {args.target}\n")

    df = get_training_data(args=args)

    data, target_encoded = create_training_data(df=df, target_label=str(args.target))

    classifiers = ["random_forest", "catboost"]

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

            mlflow.log_params(
                {f"best_{key}": value for key, value in best_trial.params.items()}
            )
            mlflow.log_metric(
                f"best_{MODEL_CONFIG.evaluation_metric}", best_trial.value
            )

    logging.info("Finished tuning.")


def objective(
    trial: optuna.Trial,
    args: ArgsTrain,
    classifier_name: str,
    data: pd.DataFrame,
    target: pd.Series,
) -> float:
    """Defines the objective function for hyperparameter optimization."""

    if classifier_name == "random_forest":
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
            "random_strength": trial.suggest_float(
                "random_strength", 1e-8, 10.0, log=True
            ),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
            "verbose": False,
            "random_state": 42,
        }
        classifier = MODEL_CONFIG.get_catboost_classifier(**param)
    else:
        raise ValueError(f"Unsupported classifier: {classifier_name}")

    model_pipeline = model(args, classifier)

    eval_metric = 0.0  # default
    with mlflow.start_run(nested=True):
        _, eval_metric = train_model(
            args=args,
            data=data,
            target_encoded=target,
            model_pipeline=model_pipeline,
            tune_model=True,
        )

    return eval_metric
