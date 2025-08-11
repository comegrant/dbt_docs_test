import logging
from typing import Literal, Optional

from openai import AsyncOpenAI
from pydantic import BaseModel, model_validator

from review_screener.config import CONTEXT, MODEL

logger = logging.getLogger(__name__)


class ReviewRequest(BaseModel):
    review: str
    source: Literal["RECIPE_RATING", "DELIVERY_RATING"]
    country: str
    rating: int
    recipe_id: Optional[int] = None
    agreement_id: Optional[int] = None
    order_id: Optional[int] = None

    @model_validator(mode="before")
    def validate_fields(cls, values):  # noqa
        source = values.get("source")

        if source == "RECIPE_RATING":
            required = ["recipe_id", "agreement_id"]
        elif source == "DELIVERY_RATING":
            required = ["order_id"]
        else:
            raise ValueError(f"Unknown source: {source}")

        missing = [field for field in required if values.get(field) is None]
        if missing:
            raise ValueError(f"Missing fields for {source}: {', '.join(missing)}")

        return values


class Response(BaseModel):
    label: bool
    score: float


class CustomerReviewResponse(Response):
    model_version: str


async def get_customer_review_response(review: str, client: AsyncOpenAI) -> Response:
    """Get a response from OpenAI for a customer review.

    Args:
        review (str): The customer review text to analyze.
        client (OpenAI): An initialized OpenAI client.

    Returns:
        Response: A Response object containing the label and score from OpenAI.

    Raises:
        ValueError: If no response is received from OpenAI.
    """
    response = await client.responses.parse(
        model=MODEL,
        input=[
            {"role": "system", "content": CONTEXT},
            {"role": "user", "content": f"Customer review: {review}"},
        ],
        text_format=Response,
    )
    if response.output_parsed is None:
        raise ValueError("No response from OpenAI")

    return response.output_parsed


async def create_customer_review_response(request: ReviewRequest, client: AsyncOpenAI) -> CustomerReviewResponse:
    """Create response for a customer review with model version information.

    Args:
        request (ReviewRequest): The review request containing review text and metadata.
        client (OpenAI): An initialized OpenAI client.

    Returns:
        CustomerReviewResponse: A response object containing the label, score, and model version.
    """
    response = await get_customer_review_response(request.review, client)
    return CustomerReviewResponse(**response.model_dump(), model_version=MODEL)


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv

    load_dotenv()

    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    request = ReviewRequest(
        review="The food was great",
        source="RECIPE_RATING",
        country="US",
        rating=5,
        recipe_id=123,
        agreement_id=456,
    )

    response = asyncio.run(create_customer_review_response(request, client))
