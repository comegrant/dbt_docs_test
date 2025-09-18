import logging

from fastapi import APIRouter, Depends, HTTPException
from menu_optimiser.common import RequestMenu, ResponseMenu
from menu_optimiser.menu_solver import build_response, generate_menu
from starlette.responses import JSONResponse

from analytics_api.utils.auth import raise_on_invalid_token

mop_router_2 = APIRouter(dependencies=[Depends(raise_on_invalid_token)])

logger = logging.getLogger(__name__)


@mop_router_2.post("/menu_2", response_model=ResponseMenu, tags=["Menu Planner"])
async def menu_generator_2(request: RequestMenu) -> JSONResponse:
    try:
        logger.info("Received request for menu generation: %s", request)

        problem, menu, errors = await generate_menu(request)

        response = build_response(problem, menu, request, errors)

        logger.info("Menu generation completed: %s", response)
        response = response.model_dump(by_alias=True, exclude_none=False)

        logger.info("Returning menu response")
        return JSONResponse(content=response, status_code=200)
    except Exception as e:
        logger.error("Menu generation failed", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
