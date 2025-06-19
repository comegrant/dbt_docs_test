import asyncio
import logging

import polars as pl
import streamlit as st
from cheffelo_logging.logging import setup_streamlit
from combinations_app import display_recipes
from data_contracts.preselector.store import SuccessfulPreselectorOutput
from data_contracts.recipe import AllergyPreferences
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


async def load_allergens() -> dict[str, str]:
    allergens = await AllergyPreferences.query().all().cached_at("data/allergy_preferences.parquet").to_polars()
    return {row["preference_id"]: row["allergy_name"] for row in allergens.to_dicts()}


async def responses_form() -> list[PreselectorSuccessfulResponse]:
    with st.form("Debug"):
        agreement_id = st.number_input("Agreement ID", min_value=0)
        reload = st.form_submit_button("Reload")

    if agreement_id <= 0:
        return []

    if not reload and str(agreement_id) in st.session_state:
        return st.session_state[str(agreement_id)]

    output = await SuccessfulPreselectorOutput.query().filter(f"billing_agreement_id = {agreement_id}").to_polars()

    structured = (
        output.group_by(["generated_at", "billing_agreement_id"])
        .agg(
            year_weeks=pl.struct(
                pl.col("menu_year"),
                pl.col("menu_week"),
                pl.col("variation_ids"),
                pl.col("main_recipe_ids"),
                pl.col("target_cost_of_food_per_recipe"),
                pl.col("compliancy"),
                pl.col("error_vector"),
            )
        )
        .sort("generated_at", descending=True)
    )

    allergens = await load_allergens()
    responses = []

    def decode_broken_taste_preferences(taste_preferences: list[str] | None) -> list[NegativePreference]:
        preferences: list[NegativePreference] = []
        if taste_preferences is None:
            return []

        for preference in taste_preferences:
            stripped = preference.removeprefix("{").removesuffix("}")

            components = stripped.split(",")
            if len(components) == 1:
                preferences.append(
                    NegativePreference(preference_id=components[0], is_allergy=components[0] in allergens)
                )
            else:
                assert len(components) == 2  # noqa
                preferences.append(
                    NegativePreference(
                        preference_id=components[0].strip('"'),
                        is_allergy=components[1],  # type: ignore
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
                recipes = [PreselectorRecipeResponse(**rec) for rec in week["recipes"]]
            else:
                recipes = [
                    PreselectorRecipeResponse(
                        main_recipe_id=recipe_id, variation_id=variation_id, compliancy=week["compliancy"]
                    )
                    for recipe_id, variation_id in zip(week["main_recipe_ids"], week["variation_ids"])
                ]

            year_weeks.append(
                PreselectorYearWeekResponse(
                    year=week["menu_year"],
                    week=week["menu_week"],
                    target_cost_of_food_per_recipe=week["target_cost_of_food_per_recipe"],
                    error_vector=week["error_vector"],
                    recipe_data=recipes,
                )
            )

        responses.append(
            PreselectorSuccessfulResponse(
                agreement_id=agreement_id,
                company_id=first_row["company_id"],
                correlation_id="",
                year_weeks=year_weeks,
                concept_preference_ids=first_row["concept_preference_ids"],
                taste_preferences=decode_broken_taste_preferences(
                    first_row.get("taste_preferences") or first_row.get("taste_preference_ids")
                ),
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


async def select(
    responses: list[PreselectorSuccessfulResponse],
) -> tuple[GenerateMealkitRequest, PreselectorYearWeekResponse] | None:
    with st.form("Select Response"):
        response = st.selectbox("Response", options=responses, format_func=lambda res: res.generated_at.isoformat())

        st.form_submit_button()

    if not response:
        return None

    company_id = response.company_id

    with st.form("Select Year Week"):
        year_week = st.selectbox(
            "Year Week", options=response.year_weeks, format_func=lambda yw: f"{yw.year}-{yw.week}"
        )
        st.form_submit_button()

    if not year_week:
        return None

    st.write("Used model")
    st.write(response.model_version)

    st.write("Negative Preferences")
    st.write(response.taste_preferences)

    allergens = await load_allergens()
    st.write("Negative allergens names")
    st.write([allergens.get(pref.preference_id) for pref in response.taste_preferences])

    st.write("Concept Preferences")
    st.write(response.concept_preference_ids)

    st.write("Compliance Value")
    st.write(year_week.compliancy)

    st.write("Variation Id")
    st.write(year_week.variation_ids)

    st.write("Recipes")
    st.write(year_week.main_recipe_ids)

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

    st.write("Quarantined Dishes")
    st.write(ordered_weeks_ago)

    return (
        GenerateMealkitRequest(
            agreement_id=response.agreement_id,
            company_id=company_id,
            compute_for=[YearWeek(year=year_week.year, week=year_week.week)],
            taste_preferences=[  # type: ignore
                pref.model_dump() for pref in response.taste_preferences
            ],
            concept_preference_ids=response.concept_preference_ids,
            number_of_recipes=len(year_week.main_recipe_ids),
            portion_size=response.portion_size,
            override_deviation=response.override_deviation,
            has_data_processing_consent=True,
            ordered_weeks_ago=ordered_weeks_ago,
        ),
        year_week,
    )


async def debug_app() -> None:
    store = preselector_store()

    async def successful_responses() -> tuple[GenerateMealkitRequest, PreselectorYearWeekResponse] | None:
        responses = await responses_form()

        if not responses:
            return None

        return await select(responses)

    async def failed_responses() -> tuple[GenerateMealkitRequest, PreselectorYearWeekResponse] | None:
        failed = await failure_responses_form()

        if not failed:
            st.write("Found no failures")
            return None

        return (
            failed[-1],
            PreselectorYearWeekResponse(year=0, week=0, target_cost_of_food_per_recipe=0, recipe_data=[]),
        )

    response = await successful_responses()
    if response is None:
        st.write("Found no results")
        return

    request, expected_recipes = response

    st.write(request)
    st.write(expected_recipes)

    cache_store = await load_cache(store, company_id=request.company_id)

    st.write(request.number_of_recipes)

    run_response = await run_preselector_for_request(request=request, store=cache_store, should_explain=True)

    if run_response.failures and run_response.success:
        failed_request = request.copy()
        failed_request.compute_for = [YearWeek(year=res.year, week=res.week) for res in run_response.failures]
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

    st.write("Ordered weeks ago:")
    st.write(success.ordered_weeks_ago)

    if set(success.main_recipe_ids) - set(expected_recipes.main_recipe_ids):
        st.header("Something is not right")
        st.error(
            f"You have drift in some way. Expected {expected_recipes.main_recipe_ids} recipe ids, "
            f"but got {success.main_recipe_ids}"
        )
        st.header("Expected the following")
        await display_recipes(expected_recipes, st)
    else:
        st.success("Output was reproduced")

    st.header("Current output")
    await display_recipes(success, st)


if __name__ == "__main__":
    preselector_logger = logging.getLogger("preselector")

    if preselector_logger.level != logging.DEBUG:
        preselector_logger.setLevel(logging.DEBUG)
        setup_streamlit(preselector_logger)

    asyncio.run(debug_app())
