"""Define FastAPI app"""

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http import HTTPStatus

from data_contracts.recommendations.store import recommendation_feature_contracts
from data_contracts.tags import Tags
from dotenv import find_dotenv, load_dotenv
from fastapi import Depends, FastAPI, Form, Request
from key_vault import key_vault

from analytics_api.routers.menu_feedback import mf_router as mf_app_router
from analytics_api.routers.menu_generator import mop_router as mop_app_router
from analytics_api.routers.menu_generator_2 import mop_router_2 as mop_app_router_2
from analytics_api.routers.recommendations import rec_router
from analytics_api.routers.review_screener import rs_router as rs_app_router
from analytics_api.settings import EnvSettings, load_settings
from analytics_api.utils.auth import AuthToken, raise_on_invalid_token, retrieve_token
from analytics_api.utils.datadog_logger import datadog_logger, setup_logger

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan hook.

    This hook is called when the application starts and shuts down.
    It is used to initialize the application and setup logging.

    Yields:
        None
    """
    from aligned.proxy_api import router_for_store

    load_dotenv(find_dotenv())

    settings = EnvSettings()

    app.dependency_overrides[load_settings] = lambda: settings

    store = recommendation_feature_contracts()

    setup_logger()
    if settings.environment != "dev":
        await datadog_logger(env=settings.environment)

    app.include_router(
        router_for_store(store, expose_tag=Tags.expose_in_analytics_api),
        dependencies=[Depends(raise_on_invalid_token)],
    )

    vault = key_vault()
    await vault.load_into_env({"openai-preselector-key": "OPENAI_API_KEY"})

    logger.info("Application started")
    yield


app = FastAPI(title="Analytics API", lifespan=lifespan)
app.include_router(mop_app_router)
app.include_router(mop_app_router_2)
app.include_router(mf_app_router)
app.include_router(rec_router)
app.include_router(rs_app_router)


@app.middleware("http")
async def monitor_processing_time(request: Request, call_next):  # noqa
    logger.info(f"Starting to process '{request.url.path}'")

    start_time = time.monotonic()
    response = await call_next(request)
    process_time = time.monotonic() - start_time

    # Using logger for now, but should move to metrics
    logger.info(f"Processing time for url '{request.url.path}' {process_time:.4f}")

    return response


@app.get("/")
async def hello() -> str:
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


@app.post("/token", include_in_schema=True)
async def login(
    username: str | None = Form(None),
    password: str | None = Form(None),
    client_secret: str | None = Form(None),
) -> AuthToken:
    """
    Authenticates a user with a username and password and returns a JSON response
    containing an access token.

    Args:
        username (str): The username to use for authentication.
        password (str): The password to use for authentication.
        client_secret (str): An optional client secret to use for authentication.

    Returns:
        A JSON response containing the access token if the authentication is
        successful, otherwise None.
    """
    if client_secret is not None:
        await raise_on_invalid_token(client_secret)
        return AuthToken(access_token=client_secret)

    assert username, f"Expected to get a username: '{username}'."
    assert password, "Expected to get a password but got nothing or an empty string."

    return await retrieve_token(username, password)
