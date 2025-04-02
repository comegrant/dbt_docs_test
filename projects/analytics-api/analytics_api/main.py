"""Define FastAPI app"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Literal

from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI, Form
from pydantic_settings import BaseSettings

from analytics_api.routers.menu_generator import mop_router as mop_app_router
from analytics_api.utils.auth import retrieve_token, validate_token
from analytics_api.utils.datadog_logger import datadog_logger

logger = logging.getLogger(__name__)


class EnvSettings(BaseSettings):
    """
    Settings for the application environment.

    Attributes:
        environment (Literal["dev", "test", "prod"]): The environment the application is running in.
            Defaults to "dev".
    """

    environment: Literal["dev", "test", "prod"] = "dev"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan hook.

    This hook is called when the application starts and shuts down.
    It is used to initialize the application and setup logging.

    Yields:
        None
    """
    load_dotenv(find_dotenv())

    settings = EnvSettings()
    await datadog_logger(env=settings.environment)
    logger.info("Application started")
    yield


app = FastAPI(title="Analytics API", lifespan=lifespan)
app.include_router(mop_app_router)


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


@app.post("/token", include_in_schema=True)
async def login(
    username: str = Form(None),
    password: str = Form(None),
    client_secret: str = Form(None),
) -> dict[str, str] | None:
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
        if await validate_token(client_secret):
            return {"access_token": client_secret}
    else:
        return await retrieve_token(username, password)
