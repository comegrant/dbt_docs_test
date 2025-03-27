import logging
from os import getenv
from typing import Any, Optional

import aiohttp
from aiocache import cached
from aiocache.serializers import PickleSerializer
from fastapi import Depends, status
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

# from pydantic.error_wrappers import ValidationError
from pydantic import ValidationError
from pydantic.types import SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# Define the OAuth2 scheme and access token URL
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class AuthConfig(BaseSettings):
    auth_host: Optional[str] = getenv("AUTH_HOST")
    auth_email: Optional[str] = getenv("AUTH_EMAIL")
    auth_password: Optional[SecretStr] = SecretStr(getenv("AUTH_PASSWORD", default=""))
    auth_public_key: Optional[Any] = getenv("AUTH_PUBLIC_KEY")
    auth_api_login: str = getenv("AUTH_API_LOGIN", default="/api/Auth/LoginBackend")
    auth_api_validate_backend: str = getenv("AUTH_API_VALIDATE_BACKEND", default="/api/Auth/ValidateTokenJose")
    auth_algorithm: str = getenv("AUTH_ALGORITHM", default="RS512")


@cached(serializer=PickleSerializer())
async def get_auth_config() -> AuthConfig | None:
    """
    Retrieve the authentication configuration for the application.

    This function is cached, so if the configuration is not available or invalid,
    it will only be logged once.

    Returns:
        AuthConfig: The authentication configuration, or None if it is invalid.
    """
    try:
        return AuthConfig()
    except ValidationError as validation_error:
        logger.error("Failed to load authentication configuration due to validation error: %s", validation_error)


async def retreive_token(username, password):  # noqa: ANN001, ANN201
    """
    Retrieves a token for a given username and password.

    Args:
        username: The username to use for authentication.
        password: The password to use for authentication.

    Returns:
        A dictionary containing the retrieved access token.

    Raises:
        HTTPException: If the authentication request fails.
    """
    auth_settings = AuthConfig()
    headers = {"X-GG-NEW": "True"}
    data = {"email": username, "password": password}
    if auth_settings.auth_host is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Auth host is not defined")
    url = auth_settings.auth_host + auth_settings.auth_api_login
    logger.info(url)

    async with aiohttp.ClientSession() as client:
        async with client.post(url, headers=headers, json=data) as response:
            if response.status == status.HTTP_200_OK:
                return {"access_token": await response.json()}

            raise HTTPException(status_code=response.status, detail=await response.json())


async def validate_token(token: str = Depends(oauth2_scheme)) -> bool:
    """
    Validate a given token.

    This function takes a token as an argument (which defaults to the token provided
    in the Authorization header of the request) and checks if it is valid. It does
    this by attempting to decode the token using the public key provided in the
    application configuration. If the decoding fails or the decoded token does not
    contain the expected CompanyId (00000000-0000-0000-0000-000000000000), an HTTP
    exception is raised with a 401 status code. Otherwise, the function returns True.

    Args:
        token (str): The token to validate. Defaults to the token provided in the
            Authorization header of the request.

    Raises:
        HTTPException: If the token is invalid or does not contain the expected
            CompanyId.

    Returns:
        bool: True if the token is valid, False otherwise.
    """
    auth_settings = AuthConfig()
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if auth_settings.auth_public_key is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Auth public key is not defined")
    try:
        payload = jwt.decode(
            token,
            auth_settings.auth_public_key,
            algorithms=[auth_settings.auth_algorithm],
        )
        company_id = payload["CompanyId"]
        if company_id != "00000000-0000-0000-0000-000000000000":
            raise credentials_exception
    except JWTError:
        raise credentials_exception from JWTError

    return True
