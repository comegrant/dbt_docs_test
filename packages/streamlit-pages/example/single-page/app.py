import asyncio

import streamlit as st
from pydantic import BaseModel
from streamlit_pages.router import DeeplinkRouter, set_deeplink


class WelcomePage(BaseModel):
    name: str


async def welcome_page(state: WelcomePage) -> None:
    st.title(f"Welcome {state.name}!")

    if st.button("Go to the next page"):
        set_deeplink(None)


async def initial_page() -> None:
    st.title("Welcome to the app")

    with st.form(key="welcome_form"):
        name = st.text_input("What is your name?")
        submit_button = st.form_submit_button("Submit")

    if submit_button and name:
        set_deeplink(WelcomePage(name=name))


if __name__ == "__main__":
    router = DeeplinkRouter(initial_page=initial_page, routes={})
    router.add_route(WelcomePage, welcome_page)

    asyncio.run(router.run())
