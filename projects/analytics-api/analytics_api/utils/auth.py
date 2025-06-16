import logging

import aiohttp
from fastapi import Depends, status
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# Define the OAuth2 scheme and access token URL
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class AuthConfig(BaseSettings):
    auth_host: str
    auth_public_key: str

    auth_api_login: str = Field("/api/Auth/LoginBackend")
    auth_algorithm: str = Field("RS512")
    auth_email: str | None = Field(None)

    @property
    def auth_login_uri(self) -> str:
        return self.auth_host + self.auth_api_login


class AuthToken(BaseModel):
    access_token: str


class TokenContent(BaseModel):
    billing_agreement_id: int = Field(alias="AgreementId")
    company_id: str = Field(alias="CompanyId")


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)


def get_auth_config() -> AuthConfig:
    """
    Retrieve the authentication configuration for the application.

    This function is cached, so if the configuration is not available or invalid,
    it will only be logged once.

    Returns:
        AuthConfig: The authentication configuration, or None if it is invalid.
    """
    try:
        return AuthConfig()  # type: ignore
    except ValidationError as validation_error:
        logger.error("Failed to load authentication configuration due to validation error: %s", validation_error)
        raise validation_error


async def retrieve_token(username: str, password: str) -> AuthToken:
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
    auth_settings = get_auth_config()

    headers = {"X-GG-NEW": "True"}
    data = {"email": username, "password": password}

    url = auth_settings.auth_login_uri

    async with aiohttp.ClientSession() as client:
        async with client.post(url, headers=headers, json=data) as response:
            if response.status != status.HTTP_200_OK:
                raise HTTPException(status_code=response.status, detail=await response.json())

            return AuthToken(access_token=await response.json())


def extract_token_content(token: str = Depends(oauth2_scheme)) -> TokenContent:
    """
    Extracts the contents of the token.

    Args:
        token (str): The token to validate. Defaults to the token provided in the
            Authorization header of the request.

    Raises:
        HTTPException: If the token is invalid, aka. unable to decode it.

    Returns:
        TokenContent: The content of the token.
    """
    auth_settings = get_auth_config()

    try:
        payload = jwt.decode(
            token,
            auth_settings.auth_public_key,
            algorithms=[auth_settings.auth_algorithm],
        )
        return TokenContent.model_validate(payload)
    except JWTError:
        raise credentials_exception from JWTError


async def raise_on_invalid_token(token: str = Depends(oauth2_scheme)) -> None:
    """
    Validates that a given token is a server-to-server token.

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
    """
    token_content = extract_token_content(token=token)

    if token_content.company_id != "00000000-0000-0000-0000-000000000000":
        raise credentials_exception
