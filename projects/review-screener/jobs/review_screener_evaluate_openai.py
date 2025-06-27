import asyncio
import logging
from contextlib import suppress
from typing import Literal

import mlflow
from pydantic import BaseModel, Field
from pydantic_argparser import parse_args
from review_screener.evaluate import evaluate_openai_response, load_eval_reviews
from slack_connector.slack_notification import send_slack_notification


class Args(BaseModel):
    environment: Literal["prod", "test", "dev"] = Field("dev")


async def main(args: Args) -> None:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment("/Shared/ml_experiments/review-screener")

    sample_fraction = 0.05
    threshold = 0.8
    environment = args.environment

    df = load_eval_reviews(fraction=sample_fraction)

    recall, precision, f1, accuracy = await evaluate_openai_response(df, threshold=threshold)

    if accuracy >= threshold:
        send_slack_notification(
            environment=environment,
            header_message="🔍 Review Screener Evaluation",
            body_message=(
                f"Evaluated review screener model on {len(df)} reviews.\n"
                f"Recall: {recall:.2%}\n"
                f"Precision: {precision:.2%}\n"
                f"F1: {f1:.2%}\n"
                f"Accuracy: {accuracy:.2%}\n"
            ),
            relevant_people="agathe",
            is_error=False,
        )
    else:
        send_slack_notification(
            environment=environment,
            header_message="⚠️ Review Screener accuracy is below 80%",
            body_message=(
                f"Evaluated review screener model on {len(df)} reviews.\n"
                f"Recall: {recall:.2%}\n"
                f"Precision: {precision:.2%}\n"
                f"F1: {f1:.2%}\n"
                f"Accuracy: {accuracy:.2%}\n"
            ),
            relevant_people="agathe",
            is_error=True,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main(args=parse_args(Args)))
