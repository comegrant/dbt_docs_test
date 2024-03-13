import asyncio

import pandas as pd
import streamlit as st
from pydantic_form import pydantic_form
from rec_engine.data.store import recommendation_feature_contracts
from rec_engine.logger import StreamlitLogger
from rec_engine.main import RunArgs, run_with_args
from rec_engine.source_selector import use_local_sources_in


def has_run(state: RunArgs) -> bool:
    return st.session_state.get(state.model_dump_json(), False)


def set_has_run(state: RunArgs) -> None:
    st.session_state[state.model_dump_json()] = True


async def view_predictions(inputs: RunArgs) -> None:
    store = recommendation_feature_contracts()
    if inputs.write_to:
        store = use_local_sources_in(store, list(store.models.keys()), inputs.write_to)

    model_to_view = st.selectbox("Select model to view", list(store.models.keys()))
    if not model_to_view:
        return

    view = store.model(model_to_view)

    entities = view.model.predictions_view.entities
    with st.form(key="entity_form"):
        entity_df = st.data_editor(
            pd.DataFrame(columns=[entity.name for entity in entities]),
            num_rows="dynamic",
        )
        submit = st.form_submit_button("View")

    if not submit or entity_df.empty:
        return

    preds = await view.predictions_for(entity_df).to_polars()

    st.dataframe(preds.to_pandas())


async def main() -> None:
    st.title("Run rec_engine")

    inputs = pydantic_form(key="run_form", model=RunArgs)
    if not inputs:
        return

    st.write(inputs)
    view_preds, predict = st.tabs(["View predictions", "Predict"])

    with view_preds:
        await view_predictions(inputs)

    with predict:
        if not st.button("Run"):
            return

        if not has_run(inputs):
            await run_with_args(inputs, logger=StreamlitLogger())
            set_has_run(inputs)


if __name__ == "__main__":
    asyncio.run(main())
