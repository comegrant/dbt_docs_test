import mlflow
import mlflow.sklearn
import pandas as pd
from pyspark.sql import SparkSession
from recipe_tagging.common import Args
from recipe_tagging.train.model import train_model


def get_labelled_data(spark: SparkSession, args: Args) -> pd.DataFrame:
    return NotImplementedError  # pyright: ignore


def train_pipeline(spark: SparkSession, args: Args) -> None:
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment("/Shared/ml_experiments/recipe-tagging")

    df = get_labelled_data(spark, args)

    with mlflow.start_run():
        pipeline, signature, input_example = train_model(df, args.language)

        mlflow.sklearn.autolog()

        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
            registered_model_name=f"{args.env}.mloutputs.recipe_tagging_{args.language}",
            signature=signature,
            input_example=input_example,
        )
        mlflow.log_param(
            "model_type", type(pipeline.named_steps["classifier"]).__name__
        )
        mlflow.log_param(
            "vectorizer", type(pipeline.named_steps["vectorizer"]).__name__
        )

    mlflow.end_run()
