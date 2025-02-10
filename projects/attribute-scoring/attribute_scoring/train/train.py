import logging

import datetime as dt
import mlflow
import pandas as pd
from attribute_scoring.common import Args
from attribute_scoring.train.configs import DataConfig, ModelConfig
from attribute_scoring.train.model import model
from attribute_scoring.train.preprocessing import prepare_training_data
from attribute_scoring.train.utils import (
    ModelWrapper,
    log_feature_importance,
    log_metrics,
)
from databricks.connect import DatabricksSession
from databricks.feature_engineering import FeatureEngineeringClient
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.pipeline import Pipeline

DATA_CONFIG = DataConfig()
MODEL_CONFIG = ModelConfig()


def train_pipeline(
    args: Args, fe: FeatureEngineeringClient, spark: DatabricksSession
) -> None:
    """Executes the training pipeline for a given company and target.

    This function prepares the training data, trains the model pipeline, and logs the final trained model with MLflow.

    Args:
        args (Args): Configuration arguments.
        fe (FeatureEngineeringClient): A feature engineering client.
        spark (DatabricksSession): A Spark session.
    """
    if args.target is None:
        raise ValueError("Target must be specified.")

    external_target_name = DATA_CONFIG.target_mapped.get(args.target)
    if external_target_name is None:
        raise ValueError("No mapping found for target")

    logging.info(
        f"\nStarting training pipeline for {args.company} ({external_target_name})\n"
    )
    logging.info("Preparing training data...")
    data, target_encoded, training_set = prepare_training_data(args, fe, spark)
    model_pipeline = model(args=args)

    logging.info("Training model...")
    with mlflow.start_run(
        run_name=f"{args.company}_{args.target}_training_run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ):
        wrapped_model, _ = train_model(
            args=args,
            data=data,
            target_encoded=target_encoded,
            model_pipeline=model_pipeline,
            tune_model=False,
        )

    logging.info("Logging final model...")
    fe.log_model(
        model=wrapped_model,
        artifact_path="pyfunc_packaged_model",
        training_set=training_set,
        flavor=mlflow.pyfunc,
        registered_model_name=f"{args.env}.mloutputs.attribute_scoring_{args.company}_{external_target_name}",
    )

    logging.info(f"\nFinished training for {args.company} ({external_target_name}).\n")
    mlflow.end_run()


def train_model(
    args: Args,
    data: pd.DataFrame,
    target_encoded: pd.Series,
    model_pipeline: Pipeline,
    tune_model: bool = False,
) -> tuple[ModelWrapper, float]:
    """Trains a machine learning model using the provided data and target.

    This functions trains a achine learning model, performs corss validaiton, and logs the model metrics.

    Args:
        args (Args): Configuration arguments.
        data (pd.DataFrame): The feature data for training.
        target_encoded (pd.Series): The encoded target variable.
        model_pipeline (Pipeline): The pipeline containing preprocessing steps and the classifier.
        tune_model (bool, optional): Whether to perform model tuning. Defaults to False.

    Returns:
        tuple[ModelWrapper, float]:
        - A wrapped pyfunc version of the trained model pipeline.
        - The evaluation metric from the test set (e.g., F1 score).

    """

    logging.info("Fitting model...")
    X_train, X_test, y_train, y_test = train_test_split(  # noqa
        data, target_encoded, test_size=0.2, random_state=42, stratify=target_encoded
    )

    metrics = ["accuracy", "f1", "precision", "recall"]
    cv_results = cross_validate(model_pipeline, X_train, y_train, cv=5, scoring=metrics)

    model_pipeline.fit(X_train, y_train)
    y_test_pred = model_pipeline.predict(X_test)

    if not tune_model:
        log_feature_importance(
            args=args,
            feature_importance=model_pipeline.named_steps[
                "classifier"
            ].feature_importances_,
            feature_names=model_pipeline.named_steps[
                "preprocessor"
            ].get_feature_names_out(),
        )

    mlflow.log_params(model_pipeline.get_params(deep=True))
    eval_metric = log_metrics(y_test, y_test_pred, cv_results)

    wrapped = ModelWrapper(model_pipeline)

    return wrapped, eval_metric
