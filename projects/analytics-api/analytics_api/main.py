"""Define FastAPI app"""

import logging
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import AsyncIterator, Literal  # noqa: UP035

from dotenv import find_dotenv, load_dotenv

# from analyticsapi.config.envtools import load_env_variables
# from analyticsapi.config.log import generate_log
# from analyticsapi.routers import (
#    forecasting,
#    management,
#    menu_optimization,
#    preferences,
#    preselector,
#    recurring,
# )
# from analyticsapi.routers.consumers import Consumers
# from analyticsapi.services.external_apis import auth_api
from fastapi import FastAPI, Form  # , Query
from fastapi.routing import APIRoute
from pydantic_settings import BaseSettings

from analytics_api.routers.menu_optimiser import mop_router as mop_app_router
from analytics_api.utils.auth import retrieve_token, validate_token
from analytics_api.utils.datadog_logger import datadog_logger

logger = logging.getLogger(__name__)


class EnvSettings(BaseSettings):
    environment: Literal["dev", "test", "prod"] = "dev"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    load_dotenv(find_dotenv())

    settings = EnvSettings()
    await datadog_logger(env=settings.environment)
    logger.info("Application started")
    yield


app = FastAPI(title="Analytics API", lifespan=lifespan)
app.include_router(mop_app_router)


# app.include_router(forecasting.router)
# app.include_router(recurring.router)
# app.include_router(management.router)
# app.include_router(preferences.router)
# app.include_router(menu_optimization.router)
# app.include_router(preselector.router)
#
# logger = generate_log("analyticsapi.main")


@app.get("/")
async def hello() -> Literal["Hello from the new site again!!"]:
    """
    Default landing page to test if page is up
    """
    string_to_return = "Hello from the new site again!!"
    return string_to_return


@app.get("/health", status_code=HTTPStatus.OK)
def perform_healthcheck() -> dict[str, str]:
    """
    Simple route to healthcheck on.
    It basically sends a GET request to the route & hopes to get a "200"
    response code. Failing to return a 200 response code just enables
    an alert to be sent from azure.
    Additionally, it also returns a JSON response in the form of:
    {
        'healtcheck': 'Everything OK!'
    }
    """
    return {"healthcheck": "Everything OK!"}


# @app.get("/OpenApi")
# async def get_specific_openapi(api_tag: str = Query(None, enum=Consumers.list())):
#    return get_openapi(
#        title="Analytics API",
#        version="1.0.0",
#        routes=[
#            route for route in app.routes if not api_tag or (isinstance(route, APIRoute) and api_tag in route.tags)
#        ],
#    )


@app.post("/token", include_in_schema=True)
async def login(
    username: str = Form(None),
    password: str = Form(None),
    client_secret: str = Form(None),
) -> dict[str, str] | None:
    if client_secret is not None:
        if await validate_token(client_secret):
            return {"access_token": client_secret}
    else:
        return await retrieve_token(username, password)


# def custom_openapi():
#    if app.openapi_schema:
#        return app.openapi_schema
#    routes = app.routes
#    old_tags = []
#    for r in routes:
#        if isinstance(r, APIRoute):
#            old_tags.append(r.tags)
#            r.tags = [tag for tag in r.tags if tag not in Consumers.list()]
#    openapi_schema = get_openapi(title="Analytics API", version="1.0.0", routes=routes)
#    i = 0
#    for r in routes:
#        if isinstance(r, APIRoute):
#            r.tags = old_tags[i]
#            i += 1
#    app.openapi_schema = openapi_schema
#    return app.openapi_schema
#
#
# app.openapi = custom_openapi


def use_route_names_as_operation_ids() -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


use_route_names_as_operation_ids()


# load environments on startup
# @app.on_event("startup")
# async def load_configurations():
#    """Load application configurations and environment variables from keyvault"""
#    await load_env_variables()
#    await auth_api.post_backend_login()
