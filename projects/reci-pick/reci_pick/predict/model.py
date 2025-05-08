import logging

import mlflow
import tensorflow as tf


def get_model_and_version(model_uri: str) -> tuple[tf.keras.Model, str]:
    trained_model = mlflow.tensorflow.load_model(model_uri)
    model_version = get_model_version(model_uri=model_uri)

    return trained_model, model_version


def get_model_version(model_uri: str) -> str:
    # if using alias, indicated by "@"
    # example: models:/dev.mloutputs.reci_pick_amk@champion'
    if "@" in model_uri:
        registry_link, alias = model_uri.split("@")
        model_name_without_prefix = registry_link.split("/")[1]
        model_details = mlflow.tracking.MlflowClient().get_model_version_by_alias(
            name=model_name_without_prefix, alias=alias
        )
        model_version = f"{model_details.name}/{model_details.version}"
    else:
        # assume model uri is in the format of models:/<name>/<version>
        try:
            _, name, version = model_uri.split("/")
            model_version = f"{name}/{version}"
        except Exception as e:
            logging.error(e)
            model_version = None
    return model_version
