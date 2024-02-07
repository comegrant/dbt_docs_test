import asyncio

import streamlit as st
from pydantic_form import pydantic_form
from rec_engine.main import RunArgs


async def main() -> None:
    st.title("Run rec_engine")

    inputs = pydantic_form(key="run_form", model=RunArgs)
    if not inputs:
        return

    st.write(inputs.model_dump_json(exclude_none=True))


if __name__ == "__main__":
    asyncio.run(main())
