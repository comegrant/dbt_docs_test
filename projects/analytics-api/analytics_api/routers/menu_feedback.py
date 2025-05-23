import logging

from fastapi import APIRouter, Depends, HTTPException
from preselector.menu_feedback import MenuFeedbackRequestModel, MenuFeedbackResponseModel, create_menu_feedback
from starlette.responses import JSONResponse

from analytics_api.utils.auth import raise_on_invalid_token

mf_router = APIRouter(dependencies=[Depends(raise_on_invalid_token)])

logger = logging.getLogger(__name__)


@mf_router.post("/menu/feedback", response_model=MenuFeedbackResponseModel, tags=["Menu Feedback"])
async def get_menu_feedback(request: MenuFeedbackRequestModel) -> JSONResponse:
    try:
        logger.info("Received request for menu feedback analysis")

        response = await create_menu_feedback(request)

        logger.info("Menu feedback analysis completed")
        response = response.model_dump(by_alias=True, exclude_none=False)
        return JSONResponse(content=response, status_code=200)

    except Exception as e:
        logger.error("Menu feedback analysis failed", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
