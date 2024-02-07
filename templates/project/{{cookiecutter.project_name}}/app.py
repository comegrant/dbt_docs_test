import asyncio
import streamlit as st
from {{cookiecutter.module_name}}.main import RunArgs, run
from pydantic_form import pydantic_form

async def main():

    st.title("{{cookiecutter.library_name}}")
    args = pydantic_form(keys="run_args", model=RunArgs)

    if not args:
        st.warning("Please fill in the form to run the app.")
        return

    st.write(f"Running the app with the following arguments: {args}")
    with st.spinner("Running the app..."):
        await run(args)


if __name__ == "__main__":
    asyncio.run(main())
