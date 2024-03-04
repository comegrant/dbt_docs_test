from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import sleep
from typing import NoReturn, TypeVar

import streamlit as st
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def has_deeplink() -> bool:
    return st.query_params.get("state") is not None


def set_deeplink(state: BaseModel | None) -> NoReturn | None:
    # Need to sleep in order to get out of the corutine, and therefore set the query parameters
    wait_duration = 0.1

    if state is None:
        if not has_deeplink():
            return

        st.query_params.clear()

        sleep(wait_duration)
        st.rerun()

    state_name = state.__class__.__name__
    with st.spinner(f"Routing to {state_name}..."):
        json_state = state.model_dump_json()

        st.query_params.clear()
        st.query_params["state"] = state_name
        st.query_params["state_json"] = json_state

        sleep(wait_duration)

    st.rerun()


def read_deeplink(state: type[T]) -> T | None:
    state_name = state.__name__
    if st.query_params.get("state") != state_name:
        return None

    state_json = st.query_params.get("state_json", "")
    return state.model_validate_json(state_json)


@dataclass
class DeeplinkRouter:
    initial_page: Callable[[], Awaitable[None]]
    routes: dict[type[BaseModel], Callable[[BaseModel], Awaitable[None]]]
    url_path: str = "/"

    async def run(self) -> None:
        if not has_deeplink():
            await self.initial_page()
        else:
            for state, handler in self.routes.items():
                state_instance = read_deeplink(state)
                if state_instance:
                    await handler(state_instance)
                    break

    def add_route(
        self,
        state: type[T],
        handler: Callable[[T], Awaitable[None]],
    ) -> None:
        self.routes[state] = handler

    def main(self):
        import asyncio

        asyncio.run(self.run())


def deeplinks(
    deeplinks: dict[type[BaseModel], Callable[[BaseModel], Awaitable[None]]],
) -> Callable[[Callable[[], Awaitable[None]]], DeeplinkRouter]:
    def decorator(func: Callable[[], Awaitable[None]]) -> DeeplinkRouter:
        return DeeplinkRouter(initial_page=func, routes=deeplinks)

    return decorator
