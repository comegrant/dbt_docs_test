from typing import Protocol


class Logger(Protocol):
    def info(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        ...

    def error(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        ...

    def debug(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        ...

    def warning(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        ...


class StreamlitLogger(Logger):
    def info(self, message: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        import streamlit as st

        st.info(message)

    def error(
        self,
        message: str,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> None:
        import streamlit as st

        st.error(message)

    def debug(
        self,
        message: str,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> None:
        import streamlit as st

        st.info(message)

    def warning(
        self,
        message: str,
        *args,  # noqa: ANN002
        **kwargs,  # noqa: ANN003
    ) -> None:
        import streamlit as st

        st.warning(message)
