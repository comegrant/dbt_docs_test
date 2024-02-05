import streamlit as st
import asyncio
from {{cookiecutter.module_name}}.main import main as main_func
from settings import Settings

async def main():
    await main_func()
    st.write("Hello, world!")
    settings = Settings()
    st.write(settings)

if __name__ == "__main__":
    asyncio.run(main())
