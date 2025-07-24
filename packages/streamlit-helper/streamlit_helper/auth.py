from typing import TYPE_CHECKING

import requests
import streamlit as st
from pydantic import BaseModel, Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings

if TYPE_CHECKING:
    from msal import ConfidentialClientApplication


class AuthenticatedUser(BaseModel):
    id: str
    surname: str
    given_name: str = Field(alias="givenName")
    display_name: str = Field(alias="displayName")

    mail: str

    business_phones: list[str] | None = Field(alias="businessPhones")
    office_location: str | None = Field(alias="officeLocation")
    preferred_language: str | None = Field(alias="preferredLanguage")


class AuthToken(BaseModel):
    access_token: str


class AuthError(BaseModel):
    error: str
    error_description: str
    trace_id: str
    correlation_id: str

    def as_streamlit(self) -> None:
        st.error(
            f"""## {self.error.replace("_", " ").capitalize()}

{self.error_description}

**Correlation id**: {self.correlation_id}

**Trace id**: {self.trace_id}
"""
        )


class AuthenticationConfig(BaseSettings):
    auth_client_id: str
    auth_secret: SecretStr

    auth_tenant_id: str = "f02c0daa-f4a6-41df-9fbb-df3be1b2b577"
    auth_redirect_uri: str = "http://localhost:8501"
    auth_state_key: str = "authenticated_user"

    def client(self) -> "ConfidentialClientApplication":
        from msal import ConfidentialClientApplication

        authority_url = f"https://login.microsoftonline.com/{self.auth_tenant_id}"
        return ConfidentialClientApplication(
            self.auth_client_id, authority=authority_url, client_credential=self.auth_secret.get_secret_value()
        )

    def auth_url(self, scopes: list[str]) -> str:
        return self.client().get_authorization_request_url(scopes, redirect_uri=self.auth_redirect_uri)

    def token_for(self, code: str, scopes: list[str]) -> AuthToken | AuthError:
        token_data = self.client().acquire_token_by_authorization_code(
            code, scopes=scopes, redirect_uri=self.auth_redirect_uri
        )
        try:
            return AuthToken.model_validate(token_data)
        except ValidationError:
            return AuthError.model_validate(token_data)


def fetch_user_data(access_token: str) -> AuthenticatedUser:
    headers = {"Authorization": f"Bearer {access_token}"}
    graph_api_endpoint = "https://graph.microsoft.com/v1.0/me"
    response = requests.get(graph_api_endpoint, headers=headers)
    return AuthenticatedUser.model_validate(response.json())


def authenticated_user(
    scopes: list[str] | None = None, config: AuthenticationConfig | None = None
) -> AuthenticatedUser:
    if config is None:
        config = AuthenticationConfig()  # type: ignore

    if config.auth_state_key in st.session_state:
        return st.session_state[config.auth_state_key]

    if scopes is None:
        scopes = ["User.Read"]

    if "code" in st.query_params:
        code = st.query_params["code"]

        token = config.token_for(code, scopes)

        if isinstance(token, AuthToken):
            user = fetch_user_data(token.access_token)
            st.session_state[config.auth_state_key] = user
            del st.query_params["code"]
            return user
        else:
            token.as_streamlit()

    auth_url = config.auth_url(scopes)
    btn_classes = (
        "flex w-full items-center justify-center gap-3 rounded-md bg-white px-3 py-2 "
        "text-sm font-semibold text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 "
        "hover:bg-gray-50 focus-visible:ring-transparent"
    )
    azure_image = (
        "https://upload.wikimedia.org/wikipedia/commons/thumb/f/fa/Microsoft_Azure.svg/2048px-Microsoft_Azure.svg.png"
    )
    st.markdown(
        f"""
        <head>
            <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        </head>
        <body>
            <a href="{auth_url}" target="_self" class="{btn_classes}">
                <img src="{azure_image}" class="h-full max-h-12"/>
                <span class="text-sm/6 font-semibold text-gray-900 no-underline">Login with Azure</span>
            </a>
        </body>
        """,
        unsafe_allow_html=True,
    )
    st.stop()
