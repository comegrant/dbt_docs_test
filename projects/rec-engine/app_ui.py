import asyncio
from contextlib import suppress

import pandas as pd
import streamlit as st
from data_contracts.recommendations.store import recommendation_feature_contracts
from data_contracts.sql_server import SqlServerDataSource
from pydantic_form import pydantic_form
from rec_engine.logger import StreamlitLogger
from rec_engine.main import RunArgs, run_with_args
from rec_engine.source_selector import use_local_sources_in


def has_run(state: RunArgs) -> bool:
    return st.session_state.get(state.model_dump_json(), False)


def set_has_run(state: RunArgs) -> None:
    st.session_state[state.model_dump_json()] = True


async def view_raw_source(inputs: RunArgs) -> None:
    store = recommendation_feature_contracts()
    if inputs.write_to:
        store = use_local_sources_in(store, list(store.models.keys()), inputs.write_to)

    model_to_view = st.selectbox(
        "Select model to view",
        list(store.models.keys()),
        key="raw_source",
    )
    if not model_to_view:
        return

    view = store.model(model_to_view)

    pred_view = view.model.predictions_view
    if not pred_view.source:
        st.write("No source found")
        return

    if not pred_view.source:
        st.write("No source found")
        return

    loaded = await view.all_predictions().to_polars()
    st.dataframe(loaded.to_pandas())


async def view_input_feature(inputs: RunArgs) -> None:
    store = recommendation_feature_contracts()
    if inputs.write_to:
        store = use_local_sources_in(store, list(store.models.keys()), inputs.write_to)

    model_to_view = st.selectbox(
        "Select model to view",
        list(store.models.keys()),
        key="input_feature",
    )

    if not model_to_view:
        return

    view = store.model(model_to_view)

    entities = view.request().request_result.entities
    with st.form(key="input_entity_form"):
        entity_df = st.data_editor(
            pd.DataFrame(columns=[entity.name for entity in entities]),
            num_rows="dynamic",
        )
        submit = st.form_submit_button("View")

    if not submit or entity_df.empty:
        return

    df = await view.features_for(entity_df).to_polars()
    st.dataframe(df.to_pandas())


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

    job = view.predictions_for(entity_df)
    st.code(job.describe(), language="sql")

    preds = await job.to_polars()

    st.dataframe(preds.to_pandas())


async def setup_ddl() -> None:
    store = recommendation_feature_contracts()

    st.title("Setup DDL")
    st.write("This is a work in progress")

    model_name = st.selectbox("Select models", list(store.models.keys()), index=None)

    if not model_name:
        return

    view = store.model(model_name)
    pred_view = view.model.predictions_view
    request = pred_view.request("")

    source = None
    if pred_view.source and isinstance(pred_view.source, SqlServerDataSource):
        source = pred_view.source
    elif pred_view.application_source and isinstance(
        pred_view.application_source,
        SqlServerDataSource,
    ):
        source = pred_view.application_source

    if not source:
        st.write("No Sql Server source found")
        return

    mssql_dtype_map = {
        "int64": "int",
        "int32": "int",
        "float64": "float",
        "bool": "bit",
        "int16": "smallint",
        "int8": "tinyint",
        "string": "nvarchar(255)",
        "datetime": "datetime",
    }

    mssql_ddl_table_create = ""

    table = source.table
    if source.config.schema:
        table = f"{source.config.schema}.{table}"

    st.write(
        "The following queries will be run on the database described at the environment var below:",
    )
    st.write(source.config.env_var)

    mssql_ddl_table_create += f"""CREATE TABLE {table} ("""
    from aligned.schemas.constraints import Optional

    inverse_map = {v: k for k, v in source.mapping_keys.items()}

    for feature in request.features.union(pred_view.entities):
        dtype = mssql_dtype_map.get(feature.dtype.name, "text").upper()
        is_optional = (
            feature.constraints is not None and Optional() in feature.constraints
        )
        if not is_optional:
            dtype = f"{dtype} NOT NULL"
        column_name = inverse_map.get(feature.name, feature.name)
        mssql_ddl_table_create += f"\n    {column_name} {dtype},"

    if request.event_timestamp:
        column_name = inverse_map.get(
            request.event_timestamp.name,
            request.event_timestamp.name,
        )
        mssql_ddl_table_create += f"\n    {column_name} DATETIME NOT NULL,"

    mssql_ddl_table_create = mssql_ddl_table_create[:-1] + "\n);"

    st.write(mssql_ddl_table_create)

    if st.button("DELETE all and recreate table"):
        con = source.config.connection.raw_connection()

        with suppress(Exception):
            con.execute(f"CREATE SCHEMA {source.config.schema};")
            con.commit()

        con.execute(f"DROP TABLE {table};")
        con.commit()
        con.execute(mssql_ddl_table_create)
        con.commit()

        res = con.execute(
            f"""SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = N'{source.table}'""",
        )
        st.write(res.fetchall())


async def main() -> None:
    st.title("Run rec_engine")

    inputs = pydantic_form(key="run_form", model=RunArgs)
    if not inputs:
        return

    st.write(inputs)
    view_preds, raw_source, ddl, predict = st.tabs(
        ["View predictions", "View raw source", "DDL", "Predict"],
    )

    with view_preds:
        await view_predictions(inputs)

    with raw_source:
        await view_raw_source(inputs)

    with ddl:
        await setup_ddl()

    with predict:
        st.write("This will run the whole recommendation engine pipeline.")
        st.write(
            "This will train and predict over the data, and can be quite resource intensive.",
        )
        st.write(
            "Therefore, make sure to either set of ≈ 10 GB of memory, "
            "or only predict for a subset of agreement ids, "
            "using the `only_for_agreement_ids` field.",
        )

        if not st.button("Run"):
            return

        if not has_run(inputs):
            await run_with_args(inputs, logger=StreamlitLogger())
            set_has_run(inputs)


if __name__ == "__main__":
    asyncio.run(main())
