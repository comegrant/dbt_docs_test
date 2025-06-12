from datetime import datetime

import mlflow
import pandas as pd
import pytz
from catalog_connector import connection
from key_vault import key_vault
from openai import OpenAI
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from review_screener.config import MODEL
from review_screener.main import Response, get_customer_review_response


def load_eval_reviews(fraction: float = 0.05) -> pd.DataFrame:
    """Loads manually annotated reviews for evaluation.

    Args:
        fraction (float): The fraction of reviews to sample.

    Returns:
        pd.DataFrame: A stratified sample of reviews.
    """
    df = connection.table("mltesting.nlp").read().toPandas()
    stratified_sample = df.groupby("needs_answer").apply(lambda x: x.sample(frac=fraction))

    return stratified_sample


async def process_reviews(df: pd.DataFrame, client: OpenAI) -> list[Response]:
    """Processes reviews through the OpenAI API.

    Args:
        df (pd.DataFrame): A DataFrame containing reviews.
        client (OpenAI): An OpenAI client.

    Returns:
        list[Response]: A list of responses from the OpenAI API.
    """
    responses = []
    for review in df["text"]:
        response = await get_customer_review_response(review, client)
        responses.append(response)
    return responses


async def evaluate_openai_response(df: pd.DataFrame, threshold: float) -> None:
    """Evaluates the OpenAI response and logs the results to Databrick experiment.

    Args:
        df (pd.DataFrame): A DataFrame containing reviews.
        threshold (float): A threshold for the evaluation.
    """
    vault = key_vault()
    await vault.load_into_env({"openai-preselector-key": "OPENAI_API_KEY"})
    client = OpenAI()

    timezone = pytz.timezone("UTC")
    timestamp_now = datetime.now(tz=timezone).strftime("%Y-%m-%d-%H:%M:%S")

    with mlflow.start_run(run_name=f"review-screener-evaluation-{timestamp_now}"):
        mlflow.log_param("model", MODEL)
        mlflow.log_param("threshold", threshold)

        responses = await process_reviews(df, client)

        df["api_response"] = responses
        df["label"] = df["api_response"].apply(lambda x: x.label)

        recall = float(recall_score(df["needs_answer"], df["label"]))
        precision = float(precision_score(df["needs_answer"], df["label"]))
        f1 = float(f1_score(df["needs_answer"], df["label"]))
        accuracy = float(accuracy_score(df["needs_answer"], df["label"]))

        mlflow.log_metric("recall", recall)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("accuracy", accuracy)

        assert recall >= threshold, f"Recall {recall:.2%} is below threshold {threshold:.2%}"
        assert precision >= threshold, f"Precision {precision:.2%} is below threshold {threshold:.2%}"
        assert f1 >= threshold, f"F1 Score {f1:.2%} is below threshold {threshold:.2%}"
        assert accuracy >= threshold, f"Accuracy {accuracy:.2%} is below threshold {threshold:.2%}"
