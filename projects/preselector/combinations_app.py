import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from time import monotonic
from types import ModuleType
from typing import TypeVar

import polars as pl
import streamlit as st
from concept_definition_app import load_attributes
from data_contracts.sources import adb
from dotenv import load_dotenv
from preselector.data.models.customer import PreselectorYearWeekResponse
from preselector.main import run_preselector_for_request
from preselector.process_stream import load_cache
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from streamlit.delta_generator import DeltaGenerator
from ui.components.mealkit import mealkit
from ui.deeplinks.compare_week import cached_recipe_info

logger = logging.getLogger(__name__)


@dataclass
class TastePref:
    preference_id: str
    name: str


async def load_taste_preferences(company_id: str) -> list[TastePref]:
    sql = f"""SELECT p.preference_id, p.name
FROM cms.preference p
INNER JOIN cms.preference_company pc ON p.preference_id = pc.preference_id
WHERE p.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB' AND (
    pc.is_active = '1' OR pc.created_at > '2024-01-01'
) AND pc.company_id = '{company_id}'"""

    df = await adb.fetch(sql).to_polars()
    return [TastePref(**row) for row in df.iter_rows(named=True)]


T = TypeVar("T")


def all_combinations(ids: list[T], include_empty_set: bool) -> list[list[T]]:
    if not ids:
        if include_empty_set:
            return [[]]
        else:
            return []

    if len(ids) == 1:
        return [*all_combinations([], include_empty_set), ids]

    new_combos = []

    last = ids[-1]

    for combo in all_combinations(ids[:-1], include_empty_set):
        new_combos.append(combo)
        new_combos.append([*combo, last])

    if not include_empty_set:
        new_combos.append([last])

    return new_combos


async def main() -> None:
    load_dotenv(".env")

    st.title("Default Pre-selector Combinations")

    store = preselector_store()

    companies = {
        "8A613C15-35E4-471F-91CC-972F933331D7": "Adams",
        "09ECD4F0-AE58-4539-8E8F-9275B1859A19": "Godt Levert",
        "6A2D0B60-84D6-4830-9945-58D518D27AC2": "Linas",
        "5E65A955-7B1A-446C-B24F-CFE576BF52D7": "RT",
    }

    company_id = st.selectbox("Company", options=companies.keys(), format_func=lambda company_id: companies[company_id])

    if not company_id:
        return

    attributes = await load_attributes(company_id)
    taste_prefs = await load_taste_preferences(company_id)

    today = date.today()

    with st.form("Attributes"):
        selected_attributes = st.multiselect("Attributes", options=attributes, format_func=lambda att: att.name)
        selected_prefs = st.multiselect("Negative Prefs", options=taste_prefs, format_func=lambda pref: pref.name)
        portion_size = st.number_input("Portion Size", min_value=1, max_value=6, value=4)
        # Modify week to accept multiple weeks
        weeks = st.multiselect(
            "Weeks",
            options=range(1, 53),
            format_func=lambda week: f"Week {week}",
            default=[(today + timedelta(weeks=i)).isocalendar().week for i in range(5)],
        )
        year = st.number_input("Year", min_value=2024, value=today.year)
        number_of_recipes = st.number_input("Number of Recipes", min_value=2, max_value=5, value=5)
        agreement_id = st.number_input("Agreement ID", min_value=1, value=None)
        ordered_weeks_ago = st.text_area("Ordered Recipe in Week")
        should_explain_internals = st.toggle("Should explain internals", value=False)

        st.form_submit_button("Generate")

    if not selected_attributes:
        return

    st.write(selected_prefs)
    st.write([att.id for att in selected_attributes])

    with st.spinner("Loading Data"):
        cached_store = await load_cache(
            store,
            company_id=company_id,
        )

    if st.button("Force Update Cache"):
        with st.spinner("Loading Data"):
            cached_store = await load_cache(store, company_id=company_id, force_load=True)

    # Extract the attributes and taste preferences from the largest combination
    atters, prefs = (selected_attributes, selected_prefs)

    st.subheader(" and ".join([attr.name for attr in atters]))
    if prefs:
        st.write("With negative prefs: " + " and ".join([pref.name for pref in prefs]))

    allergy_preferences = ["fish", "gluten", "shellfish", "nuts", "egg", "lactose", "diary"]

    recipes_at_week: dict[int, int] = json.loads(str(ordered_weeks_ago)) if ordered_weeks_ago else {}

    for week in weeks:
        st.write(f"Week {week}")
        request = GenerateMealkitRequest(
            # Agreement ID is unrelevant in this scenario as `has_data_processing_concent` is False
            agreement_id=agreement_id or 1,
            company_id=company_id,
            compute_for=[YearWeek(year=year, week=week)],
            concept_preference_ids=[attr.id for attr in atters],
            taste_preferences=[
                NegativePreference(
                    preference_id=pref.preference_id, is_allergy=pref.name.lower() in allergy_preferences
                )
                for pref in prefs
            ],
            number_of_recipes=int(number_of_recipes),
            portion_size=int(portion_size),
            has_data_processing_consent=agreement_id is not None,
            ordered_weeks_ago=recipes_at_week,
            override_deviation=False,
        )

        start_time = monotonic()
        response = await run_preselector_for_request(request, cached_store, should_explain=should_explain_internals)
        end_time = monotonic()

        st.write(f"Used {(end_time - start_time):.5f}s")

        if response.success:
            await display_recipes(response.success[0], st)
            for recipe_id in response.success[0].main_recipe_ids:
                recipes_at_week[recipe_id] = year * 100 + week
        else:
            st.error(response.failures[0].error_message)


async def display_recipes(response: PreselectorYearWeekResponse, col: DeltaGenerator | ModuleType) -> None:
    st.write(response.compliancy)

    with st.spinner("Loading recipe information..."):
        pre_selector_recipe_info = await cached_recipe_info(
            main_recipe_ids=response.main_recipe_ids,
            year=response.year,
            week=response.week,
        )
        rank = pl.DataFrame(
            {
                "main_recipe_id": [rec.main_recipe_id for rec in response.recipe_data],
                "compliancy": [rec.compliancy for rec in response.recipe_data],
                "rank": range(len(response.main_recipe_ids)),
            }
        ).to_pandas()

        pre_selector_recipe_info = pre_selector_recipe_info.merge(rank, on="main_recipe_id").sort_values(
            "rank", ascending=True, ignore_index=True
        )

    with st.spinner("Displaying mealkit"):
        mealkit(pre_selector_recipe_info, col)


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
