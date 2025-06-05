import pytest
from openai import OpenAI
from pytest_mock import MockFixture
from review_screener.main import CustomerReviewResponse, Response, ReviewRequest, create_customer_review_response


@pytest.mark.asyncio
async def test_feedback_with_mocked_response(mocker: MockFixture) -> None:
    mock_response_data = {"label": 0, "score": 0.95}

    mocker.patch(
        "review_screener.main.get_customer_review_response",
        return_value=Response(**mock_response_data),
    )

    request = ReviewRequest(
        review="The food was great",
        source="RECIPE_RATING",
        country="US",
        rating=5,
        recipe_id=123,
        agreement_id=456,
    )

    client = OpenAI(api_key="test")
    result = await create_customer_review_response(request, client=client)

    assert isinstance(result, CustomerReviewResponse)
    assert result.label == 0
    assert result.score == 0.95
