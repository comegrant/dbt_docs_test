import logging

import datetime as dt
import mlflow
import pandas as pd
from attribute_scoring.common import ArgsTrain
from attribute_scoring.train.configs import ModelConfig, DataConfig
from attribute_scoring.train.model import model, ModelWrapper
from attribute_scoring.train.preprocessing import create_training_data
from attribute_scoring.train.utils import log_feature_importance, log_metrics
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.pipeline import Pipeline
from attribute_scoring.train.data import get_training_data
from model_registry import databricks_model_registry, ModelRegistryBuilder
from mlflow.models.signature import infer_signature


DATA_CONFIG = DataConfig()
MODEL_CONFIG = ModelConfig()


async def train_pipeline(
    args: ArgsTrain,
    registry: ModelRegistryBuilder = databricks_model_registry(),
) -> None:
    """Executes the training pipeline for a given company and target.

    This function prepares the training data, trains the model pipeline, and logs the final trained model with MLflow.

    Args:
    """

    external_target_name = DATA_CONFIG.target_mapped.get(args.target)
    if external_target_name is None:
        raise ValueError("No mapping found for target")

    logging.info(
        f"\nStarting training pipeline for {args.company} ({external_target_name})\n"
    )
    logging.info("Preparing training data...")
    df = get_training_data(args=args)
    data, target_encoded = create_training_data(
        df=df,
        target_label=str(args.target),
    )

    logging.info("Training model...")
    model_pipeline = model(args=args)

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
        sample_predictions = wrapped_model.predict(
            context=None, model_input=data.head(100)
        )
        signature = infer_signature(
            model_input=data.head(100), model_output=sample_predictions
        )
        model_name = f"{args.env}.mloutputs.attribute_scoring_{args.company}_{external_target_name}"

        features = []
        for feature in DATA_CONFIG.recipe_features.feature_names:
            features.append(
                f"{DATA_CONFIG.recipe_features.schema_name}.{DATA_CONFIG.recipe_features.table_name}.{feature}"
            )
        for feature in DATA_CONFIG.ingredient_features.feature_names:
            features.append(
                f"{DATA_CONFIG.ingredient_features.schema_name}.{DATA_CONFIG.ingredient_features.table_name}.{feature}"
            )

        await (
            registry.signature(signature)
            .training_dataset(data)
            .feature_references(features)
            .alias(args.alias)
            .register_as(model_name, wrapped_model)  # type: ignore
        )

    logging.info(f"\nFinished training for {args.company} ({external_target_name}).\n")
    mlflow.end_run()


def train_model(
    args: ArgsTrain,
    data: pd.DataFrame,
    target_encoded: pd.Series,
    model_pipeline: Pipeline,
    tune_model: bool = False,
) -> tuple[ModelWrapper, float]:
    """Trains a machine learning model using the provided data and target.

    This functions trains a achine learning model, performs cross validation, and logs the model metrics.

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

    print("Fitting model...")
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
