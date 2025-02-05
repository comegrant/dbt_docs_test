
import asyncio
import logging

import polars as pl
import streamlit as st
from cheffelo_logging.logging import setup_streamlit
from combinations_app import display_recipes
from data_contracts.preselector.store import SuccessfulPreselectorOutput
from preselector.data.models.customer import (
    PreselectorRecipeResponse,
    PreselectorSuccessfulResponse,
    PreselectorYearWeekResponse,
)
from preselector.main import run_preselector_for_request
from preselector.process_stream import load_cache
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store


async def failure_responses_form() -> list[GenerateMealkitRequest]:
    raise NotImplementedError(
        "Not implemented, as the current data model for failed requests is not good enough. "
        "We should enable to filter on users without decoding the JSON request"
    )


async def responses_form() -> list[PreselectorSuccessfulResponse]:


    with st.form("Debug"):
        agreement_id = st.number_input("Agreement ID", min_value=0)
        reload = st.form_submit_button("Reload")

    if agreement_id <= 0:
        return []

    if not reload and str(agreement_id) in st.session_state:
        return st.session_state[str(agreement_id)]

    output = await SuccessfulPreselectorOutput.query().filter(
        pl.col("billing_agreement_id") == agreement_id
    ).to_polars()

    structured = output.group_by(["generated_at", "billing_agreement_id"]).agg(
        year_weeks=pl.struct(
            pl.col("menu_year"),
            pl.col("menu_week"),
            pl.col("variation_ids"),
            pl.col("main_recipe_ids"),
            pl.col("target_cost_of_food_per_recipe"),
            pl.col("compliancy"),
            pl.col("error_vector"),
        )
    ).sort("generated_at", descending=True)

    responses = []

    def decode_broken_taste_preferences(taste_preferences: str) -> list[NegativePreference]:
        preferences: list[NegativePreference] = []

        for preference in taste_preferences:
            stripped = preference.removeprefix("{").removesuffix("}")

            components = stripped.split(",")
            assert len(components) == 2 # noqa
            preferences.append(
                NegativePreference(
                    preference_id=components[0].strip("\""),
                    is_allergy=components[1], # type: ignore
                )
            )
        return preferences



    for row in structured.iter_rows(named=True):
        first_row = output.filter(
            (pl.col("billing_agreement_id") == row["billing_agreement_id"])
            & (pl.col("generated_at") == row["generated_at"])
        ).rows(named=True)[0]

        year_weeks = []

        for week in row["year_weeks"]:
            if "recipes" in week:
                recipes = [
                    PreselectorRecipeResponse(**rec)
                    for rec in week["recipes"]
                ]
            else:
                recipes = [
                    PreselectorRecipeResponse(
                        main_recipe_id=recipe_id,
                        variation_id="",
                        compliancy=week["compliancy"]
                    )
                    for recipe_id in week["main_recipe_ids"]
                ]

            year_weeks.append(
                PreselectorYearWeekResponse(
                    year=week["menu_year"],
                    week=week["menu_week"],
                    target_cost_of_food_per_recipe=week["target_cost_of_food_per_recipe"],
                    error_vector=week["error_vector"],
                    recipes_data=recipes
                )
            )

        responses.append(
            PreselectorSuccessfulResponse(
                agreement_id=agreement_id,
                company_id=first_row["company_id"],
                correlation_id="",
                year_weeks=year_weeks,
                concept_preference_ids=first_row["concept_preference_ids"],
                taste_preferences=decode_broken_taste_preferences(first_row["taste_preferences"]),
                override_deviation=False,
                model_version=first_row["model_version"],
                generated_at=row["generated_at"],
                has_data_processing_consent=first_row["has_data_processing_consent"],
                number_of_recipes=first_row["number_of_recipes"],
                portion_size=first_row["portion_size"],
            )
        )

    st.session_state[str(agreement_id)] = responses
    return responses


def select(responses: list[PreselectorSuccessfulResponse]) -> tuple[GenerateMealkitRequest, list[int]] | None:

    with st.form("Select Response"):

        response = st.selectbox("Response", options=responses, format_func=lambda res: res.generated_at)

        st.form_submit_button()


    if not response:
        return None

    company_id = response.company_id

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


    st.write("Negative Preferences")
    st.write(response.taste_preferences)

    st.write("Concept Preferences")
    st.write(response.concept_preference_ids)

    st.write("Compliance Value")
    st.write(year_week.compliancy)

    st.write("Error Vector")
    st.write(year_week.error_vector)

    assert response.portion_size

    ordered_weeks_ago = year_week.ordered_weeks_ago
    if ordered_weeks_ago is None:
        ordered_weeks_ago = {}
        for week in response.year_weeks:
            this_week = week.year * 100 + week.week
            if this_week < year_week.year * 100 + year_week.week:
                for recipe_id in week.main_recipe_ids:
                    ordered_weeks_ago[recipe_id] = this_week

        if not ordered_weeks_ago:
            ordered_weeks_ago = None

    st.write("Quarentined Dishes")
    st.write(ordered_weeks_ago)

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
            portion_size=response.portion_size,
            override_deviation=response.override_deviation,
            has_data_processing_consent=True,
            ordered_weeks_ago=ordered_weeks_ago
        ),
        year_week.main_recipe_ids
    )


async def debug_app() -> None:
    store = preselector_store()

    async def successful_responses() -> tuple[GenerateMealkitRequest, list[int]] | None:
        responses = await responses_form()

        if not responses:
            return None

        return select(responses)

    async def failed_responses() -> tuple[GenerateMealkitRequest, list[int]] | None:
        failed = await failure_responses_form()

        if not failed:
            st.write("Found no failures")
            return None

        return (failed[-1], [])


    response = await successful_responses()
    if response is None:
        st.write("Found no results")
        return


    request, expected_recipes = response

    st.write(request)
    st.write(expected_recipes)

    cache_store = await load_cache(
        store, company_id=request.company_id
    )


    st.write(request.number_of_recipes)

    run_response = await run_preselector_for_request(
        request=request, store=cache_store, should_explain=True
    )

    if run_response.failures and run_response.success:
        failed_request = request.copy()
        failed_request.compute_for = [
            YearWeek(year=res.year, week=res.week)
            for res in run_response.failures
        ]
        st.write("Failed Requests")
        st.write(failed_request)
        new_run_response = await run_preselector_for_request(
            request=failed_request, store=cache_store, should_explain=True
        )
        st.write(new_run_response.failures)
        st.write(new_run_response.success)
        return




    if not run_response.success:
        st.error(run_response.failures[0])
        return

    success = run_response.success[0]

    if (set(run_response.success[0].main_recipe_ids) - set(expected_recipes)):
        st.header("Something is not right")
        st.error(
            f"You have drift in some way. Expected {expected_recipes} recipe ids, "
            f"but got {run_response.success[0].main_recipe_ids}"
        )
        st.header("Expected the following")
        await display_recipes(
            PreselectorYearWeekResponse(
                year=success.year,
                week=success.week,
                recipes_data=success.recipes_data,
                target_cost_of_food_per_recipe=0,
            ),
            st
        )


    st.header("Current output")
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
