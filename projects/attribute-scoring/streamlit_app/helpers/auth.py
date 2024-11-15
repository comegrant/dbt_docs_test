import os
from typing import Optional

import requests
import streamlit as st
from msal import ConfidentialClientApplication


def initialize_app() -> ConfidentialClientApplication:
    client_id = os.getenv("CLIENT_ID_STREAMLIT")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_secret = os.getenv("CLIENT_SECRET_STREAMLIT")

    authority_url = f"https://login.microsoftonline.com/{tenant_id}"
    return ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)


def acquire_access_token(app: ConfidentialClientApplication, code: str, scopes: list[str], redirect_uri: str) -> dict:
    return app.acquire_token_by_authorization_code(code, scopes=scopes, redirect_uri=redirect_uri)


def fetch_user_data(access_token: str) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    graph_api_endpoint = "https://graph.microsoft.com/v1.0/me"
    response = requests.get(graph_api_endpoint, headers=headers)
    return response.json()


def authentication_process(app: ConfidentialClientApplication) -> Optional[dict]:
    scopes = ["User.Read"]
    redirect_uri = "https://attribute-scoring-fqfzhwg2dbg7a3ar.northeurope-01.azurewebsites.net/"
    auth_url = app.get_authorization_request_url(scopes, redirect_uri=redirect_uri)
    st.markdown(
        """
                <script src="https://cdn.tailwindcss.com"></script>
                <style>
                .grants-button {
                    background-color: initial;
                    border: 1px auto solid;
                    border-width: thin;
                    color: auto;
                    padding: 4px 12px;
                    text-align: center;
                    text-decoration: none;
                    display: inline-block;
                    font-size: 16px;
                    margin: 4px 2px;
                    cursor: pointer;
                    width: 150px;
                    border-radius: 10px;
                }
                .grants-button:hover {
                    color: #FF4B4B;
                    background-color: auto;
                    border: 1px #FF4B4B solid;
                }
                a {
                    text-decoration: none;
                }
                a:hover {
                    text-decoration: none;
                    color: red;
                }
                </style>
                """
        + f"""
                <body>
                    <a href="{auth_url}" target="_self">
                        <button class="grants-button">
                            Authenticate 
                        </button>
                    </a>
                </body>
                """,
        unsafe_allow_html=True,
    )
    if st.query_params:
        st.session_state["auth_code"] = st.query_params["code"]
        token_result = acquire_access_token(app, st.session_state.auth_code, scopes, redirect_uri=redirect_uri)
        if "access_token" in token_result:
            user_data = fetch_user_data(token_result["access_token"])
            return user_data


def login_ui() -> None:
    with st.container():
        if not st.session_state.get("authenticated"):
            st.write("Please authenticate to get started!")
            app = initialize_app()
            user_data = authentication_process(app=app)
            if user_data:
                st.session_state["authenticated"] = True
                st.session_state["user_name"] = user_data.get("displayName")
                st.session_state["user_email"] = user_data.get("mail")
                st.rerun()
