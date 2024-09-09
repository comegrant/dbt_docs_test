import inspect
from collections.abc import Awaitable, Callable


def tab_index(tabs: list[str], active_index: int = 0) -> int:
    """
    Returns the index of the active tab
    """
    from streamlit.components.v1 import declare_component

    tab_com = declare_component("tabs", path="./tabs")

    value = tab_com(tabs=tabs, active_index=active_index, key=",".join(tabs))

    if value is not None:
        return value
    return active_index


def tabs(
    tabs: list[tuple[str, Callable[[], Awaitable[None]] | Callable[[], None]]], active_index: int = 0
) -> Callable[[], Awaitable[None]] | Callable[[], None]:
    """
    Returns the index of the active tab
    """

    tab_names = [name for name, _ in tabs]
    tab_functions = [func for _, func in tabs]

    index = tab_index(tab_names, active_index=active_index)

    return tab_functions[index]


async def run_active_tab(
    tab_functions: list[tuple[str, Callable[[], Awaitable[None]] | Callable[[], None]]], active_index: int = 0
) -> None:
    """
    Runs the active tab
    """
    func = tabs(tabs=tab_functions, active_index=active_index)

    if inspect.iscoroutinefunction(func):
        await func()
    else:
        func()
