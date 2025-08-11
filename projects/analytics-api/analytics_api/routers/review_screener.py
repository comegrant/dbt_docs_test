"""
Review Screener API
"""

import logging

from fastapi import APIRouter, Depends
from openai import AsyncOpenAI
from review_screener.main import CustomerReviewResponse, ReviewRequest, create_customer_review_response

from analytics_api.utils.auth import raise_on_invalid_token

rs_router = APIRouter(dependencies=[Depends(raise_on_invalid_token)])

logger = logging.getLogger(__name__)


@rs_router.post("/review-screener", response_model=CustomerReviewResponse, tags=["Review Screener"])
async def get_review_screener_response(request: ReviewRequest) -> CustomerReviewResponse:
    logger.info("Received request for customer review response")

    client = AsyncOpenAI()
    response = await create_customer_review_response(request, client)

    logger.info(msg={"request": request.model_dump(), "response": response.model_dump()})
    return response
