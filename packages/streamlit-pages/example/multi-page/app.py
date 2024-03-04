import asyncio

import streamlit as st


def page_menu():
    st.sidebar.page_link("app.py", label="Main")
    st.sidebar.page_link("pages/other.py", label="Other")


async def main():
    page_menu()

    st.write(st.runtime.get_instance())

    st.title("Welcome to the main app")

    st.link_button("Other App", "other")


if __name__ == "__main__":
    asyncio.run(main())
