import asyncio
import logging
from contextlib import suppress

import streamlit as st
from cheffelo_logging.logging import setup_streamlit
from combinations_app import display_recipes
from data_contracts.preselector.store import Preselector
from preselector.data.models.customer import PreselectorFailedResponse, PreselectorSuccessfulResponse
from preselector.main import run_preselector_for_request
from preselector.process_stream import load_cache
from preselector.schemas.batch_request import GenerateMealkitRequest, YearWeek
from preselector.store import preselector_store
from pydantic import ValidationError


async def failure_responses_form() -> list[GenerateMealkitRequest]:

    stream = Preselector.metadata.stream_source
    assert stream is not None


    with st.form("Debug"):
        agreement_id = st.number_input("Agreement ID", min_value=0)
        reload = st.form_submit_button("Reload")

    if agreement_id <= 0:
        return []

    if not reload and str(agreement_id) in st.session_state:
        return st.session_state[str(agreement_id)]

    reader = stream.consumer("0-0")
    records = await reader.read()

    decoded_records: list[PreselectorFailedResponse] = []
    for record in records:
        with suppress(ValidationError):
            decoded_records.append(PreselectorFailedResponse.model_validate_json(record["json_data"], strict=True))

    agreement_records = [
        record.request
        for record in decoded_records
        if record.request.agreement_id == agreement_id
    ]
    st.session_state[str(agreement_id)] = agreement_records
    return agreement_records

async def responses_form() -> list[PreselectorSuccessfulResponse]:

    stream = Preselector.metadata.stream_source
    assert stream is not None


    with st.form("Debug"):
        agreement_id = st.number_input("Agreement ID", min_value=0)
        reload = st.form_submit_button("Reload")

    if agreement_id <= 0:
        return []

    if not reload and str(agreement_id) in st.session_state:
        return st.session_state[str(agreement_id)]

    reader = stream.consumer("0-0")
    records = await reader.read()

    decoded_records = []
    for record in records:
        with suppress(ValidationError):
            decoded_records.append(PreselectorSuccessfulResponse.model_validate_json(record["json_data"], strict=True))

    agreement_records = [
        record
        for record in decoded_records
        if record.agreement_id == agreement_id
    ]
    st.session_state[str(agreement_id)] = agreement_records
    return agreement_records

def select(responses: list[PreselectorSuccessfulResponse]) -> tuple[GenerateMealkitRequest, list[int]] | None:

    companies = {
        "8A613C15-35E4-471F-91CC-972F933331D7": "Adams",
        "09ECD4F0-AE58-4539-8E8F-9275B1859A19": "Godt Levert",
        "6A2D0B60-84D6-4830-9945-58D518D27AC2": "Linas",
        "5E65A955-7B1A-446C-B24F-CFE576BF52D7": "RT",
    }
    with st.form("Select Response"):

        company_id = st.selectbox(
            "Company",
            options=companies.keys(),
            format_func=lambda company_id: companies[company_id],
            index=None
        )

        response = st.selectbox("Response", options=responses, format_func=lambda res: res.generated_at)

        st.form_submit_button()


    if not response or not company_id:
        return None

    with st.form("Select Year Week"):

        year_week = st.selectbox(
            "Year Week",
            options=response.year_weeks,
            format_func=lambda yw: f"{yw.year}-{yw.week}"
        )
        st.form_submit_button()

    if not year_week:
        return None

    st.write("Used model")
    st.write(response.model_version)

    st.write("Quarentined Dishes")
    st.write(year_week.generated_recipe_ids)

    st.write("Negative Preferences")
    st.write(response.taste_preferences)

    st.write("Concept Preferences")
    st.write(response.concept_preference_ids)

    st.write("Compliance Value")
    st.write(year_week.compliancy)

    return (
        GenerateMealkitRequest(
            agreement_id=response.agreement_id,
            company_id=company_id,
            compute_for=[
                YearWeek(year=year_week.year, week=year_week.week)
            ],
            taste_preferences=[ # type: ignore
                pref.model_dump() for pref in response.taste_preferences
            ],
            concept_preference_ids=response.concept_preference_ids,
            number_of_recipes=len(year_week.main_recipe_ids),
            portion_size=year_week.portion_size,
            override_deviation=response.override_deviation,
            has_data_processing_consent=True,
            quarentine_main_recipe_ids=None,
            ordered_weeks_ago=year_week.generated_recipe_ids
        ),
        year_week.main_recipe_ids
    )


async def debug_app() -> None:
    store = preselector_store()

    responses = await responses_form()

    if not responses:
        return

    request = responses[-1]
    expected_recipes = []
    selection = select(responses)

    if selection is None:
        return

    request, expected_recipes = selection

    cache_store = await load_cache(
        store, company_id=request.company_id
    )

    st.write(request.number_of_recipes)

    run_response = await run_preselector_for_request(
        request=request, store=cache_store, should_explain=True
    )


    if not run_response.success:
        st.error(run_response.failures[0])
        return

    if (set(run_response.success[0].main_recipe_ids) - set(expected_recipes)):
        st.header("Something is not right")
        st.error(
            f"You have drift in some way. Expected {expected_recipes} recipe ids, "
            f"but got {run_response.success[0].main_recipe_ids}"
        )


    await display_recipes(
        run_response.success[0],
        st
    )


if __name__ == "__main__":
    preselector_logger = logging.getLogger("preselector")

    if preselector_logger.level != logging.DEBUG:
        preselector_logger.setLevel(logging.DEBUG)
        setup_streamlit(preselector_logger)

    asyncio.run(debug_app())
