import asyncio

import streamlit as st
from customer_churn.main import RunArgs, run
from pydantic_form import pydantic_form

"""
- churn application
- predict on user id
- stats over predicted customers
- model results?
"""


async def main() -> None:
    st.title("customer_churn")
    args = pydantic_form(key="run_args", model=RunArgs)

    if not args:
        st.warning("Please fill in the form to run the app.")
        return

    st.write(f"Running the app with the following arguments: {args}")
    with st.spinner("Running the app..."):
        await run(args)


if __name__ == "__main__":
    asyncio.run(main())
