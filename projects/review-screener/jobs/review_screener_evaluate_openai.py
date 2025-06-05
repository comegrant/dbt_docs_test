# Databricks notebook source
# COMMAND ----------

from typing import TYPE_CHECKING

from databricks_env import auto_setup_env

if TYPE_CHECKING:
    from databricks.sdk.dbutils import RemoteDbUtils

    dbutils: RemoteDbUtils = ""

auto_setup_env()

# COMMAND ----------
import pandas as pd
from key_vault import key_vault
from openai import OpenAI
from review_screener.main import Response, get_customer_review_response
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

threshold = 0.8


async def process_reviews(df: pd.DataFrame, client: OpenAI) -> list[Response]:
    responses = []
    for review in df["review"]:
        response = await get_customer_review_response(review, client)
        responses.append(response)
    return responses


async def main() -> None:
    vault = key_vault()
    await vault.load_into_env({"openai-preselector-key": "OPENAI_API_KEY"})

    client = OpenAI()

    df = pd.DataFrame(
        {
            "review": [
                # Positive reviews
                "God middag, men vanskelig å panere lakseburgerne uten at de gikk i oppløsning.",
                "Masse god smak",
                "Innen tidsfristen og rett sted👍",
                "Tipp topp!",
                "Hadde vært fint med mer variasjon i salatutvalget. Rucola er ikke for alle??",
                # Negative reviews
                "Ikke levere på gata ute når man bor midt i byen i en bygård!! Den stod ved utgangsdøra rett oppå "
                "sigarettstumperog skitt! Kom også lang tid etter å få beskjed om at den ble levert snart.",
                "Hull på sesamfrøposen. Eplesalaten var kjempegod. Sausene/dressinger for sterke og unødvendige.",
                "Hjertesalaten var ikke helt fin",
                "<br>Både rødløken og gulløken i matkassa var råtne og måtte erstattes.</p>",
                "Dere leverer på feil dør. Bruk hoveddør med ringeklokke 😊",
            ],
            "is_requiring_attention": [0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
        }
    )

    responses = await process_reviews(df, client)

    df["api_response"] = responses
    df["label"] = df["api_response"].apply(lambda x: x.label)

    recall = recall_score(df["is_requiring_attention"], df["label"])
    precision = precision_score(df["is_requiring_attention"], df["label"])
    f1 = f1_score(df["is_requiring_attention"], df["label"])
    accuracy = accuracy_score(df["is_requiring_attention"], df["label"])

    assert recall > threshold
    assert precision > threshold
    assert f1 > threshold
    assert accuracy > threshold


# COMMAND ----------
await main()  # type: ignore
