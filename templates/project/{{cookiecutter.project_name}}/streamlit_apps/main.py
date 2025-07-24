import logging

import streamlit as st
from streamlit_helper import setup_streamlit, authenticated_user


logger = logging.getLogger(__name__)


@setup_streamlit()
async def main() -> None:
    st.header("My awesome app")

    user = authenticated_user()

    st.write(f"Welcome {user.display_name}!")


if __name__ == "__main__":
    main() # type: ignore
