import asyncio
import logging
from contextlib import suppress

import mlflow
from review_screener.evaluate import evaluate_openai_response, load_eval_reviews


async def main() -> None:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment("/Shared/ml_experiments/review-screener")

    sample_fraction = 0.05
    threshold = 0.8

    df = load_eval_reviews(fraction=sample_fraction)
    await evaluate_openai_response(df, threshold=threshold)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    with suppress(ImportError):
        import nest_asyncio

        nest_asyncio.apply()

    asyncio.run(main())
