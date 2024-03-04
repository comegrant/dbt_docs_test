import asyncio

import streamlit as st
from streamlit_pages.router import deeplinks


@deeplinks({})
async def other_page():
    st.title("Welcome to the other page")


if __name__ == "__main__":
    asyncio.run(other_page.run())
