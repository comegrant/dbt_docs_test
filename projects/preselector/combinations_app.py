import asyncio
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import ModuleType
from typing import TypeVar

import pandas as pd
import streamlit as st
from aligned import FeatureLocation
from combinations_store_output import CombinationsAppOutput
from concept_definition_app import load_attributes
from data_contracts.preselector.store import (
    ImportanceVector,
    PartitionedRecommendations,
    TargetVectors,
)
from data_contracts.sources import adb
from dotenv import load_dotenv
from preselector.data.models.customer import PreselectorYearWeekResponse
from preselector.main import run_preselector_for_request
from preselector.process_stream import load_cache
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from streamlit.delta_generator import DeltaGenerator
from streamlit_cookies_manager import EncryptedCookieManager
from ui.components.mealkit import mealkit
from ui.deeplinks.compare_week import cached_recipe_info


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
        week = st.number_input("Week", min_value=1, max_value=52, value=today.isocalendar().week + 2)
        year = st.number_input("Year", min_value=2024, value=today.year)
        number_of_recipes = st.number_input("Number of Recipes", min_value=2, max_value=5, value=5)
        submit_button = st.form_submit_button("Generate")

    if not selected_attributes:
        return

    with st.spinner("Loading Data"):
        cached_store = await load_cache(
            store,
            company_id=company_id,
            exclude_views={
                FeatureLocation.model("rec_engine"),
                PartitionedRecommendations.location,
                TargetVectors.location,
                ImportanceVector.location,
            },
        )

    # Generate all combinations of selected attributes and taste preferences
    attribute_combinations = all_combinations(selected_attributes, include_empty_set=False)
    taste_preference_combinations = all_combinations(selected_prefs, include_empty_set=True)

    # Combine attribute and taste preference combinations
    combinations = [(atters, prefs) for atters in attribute_combinations for prefs in taste_preference_combinations]

    # Get the largest combination
    largest_combination = max(combinations, key=lambda x: len(x[0]) + len(x[1]))

    # Extract the attributes and taste preferences from the largest combination
    atters, prefs = largest_combination

    st.subheader(" and ".join([attr.name for attr in atters]))
    if prefs:
        st.write("With negative prefs: " + " and ".join([pref.name for pref in prefs]))

    request = GenerateMealkitRequest(
        # Agreement ID is unrelevant in this scenario as `has_data_processing_concent` is False
        agreement_id=1,
        company_id=company_id,
        compute_for=[YearWeek(year=int(year), week=int(week))],
        concept_preference_ids=[attr.id for attr in atters],
        taste_preferences=[NegativePreference(preference_id=pref.preference_id, is_allergy=True) for pref in prefs],
        number_of_recipes=int(number_of_recipes),
        portion_size=int(portion_size),
        has_data_processing_consent=False,
        override_deviation=False,
    )

    response = await run_preselector_for_request(request, cached_store)

    if response.success:
        await display_recipes(response.success[0], st)
        recipes = response.success[0]

    else:
        st.error(response.failures[0].error_message)
        recipes = []

    # Initialize the cookie manager
    cookies = EncryptedCookieManager(prefix="preselector_combinations_app", password="my-password")

    # Load the cookie
    if not cookies.ready():
        st.stop()

    # Check if session_id is in the cookie
    if "session_id" not in cookies:
        # Generate a new session_id and set it in the cookie
        session_id = str(uuid.uuid4())
        cookies["session_id"] = session_id
        cookies.save()
    else:
        # Get the session_id from the cookie
        session_id = cookies["session_id"]

    # Display a button to rate the output
    if "rating" not in st.session_state:
        st.session_state["rating"] = None

    # Reset the rating when Generate is clicked
    if submit_button:
        st.session_state["rating"] = None

    col1, col2 = st.columns(2)
    with col1:
        if st.button("👍 Good 😊"):
            st.session_state["rating"] = "Good"
    with col2:
        if st.button("👎 Bad 😕"):
            st.session_state["rating"] = "Bad"

    if st.session_state["rating"]:
        st.write(f"You rated the output as {st.session_state['rating']}.")

        # Create a pandas ataframe to store the output

        output_df = pd.DataFrame(
            {
                "identifier": str(uuid.uuid4()),
                "session_info": [session_id],
                "time": [datetime.now(timezone.utc)],
                "company_id": [company_id],
                "year": [year],
                "week": [week],
                "recipes": [recipes.main_recipe_ids],
                "rating": [st.session_state.get("rating")],
                "attributes": [[attr.name for attr in atters]],
                "taste_preferences": [[pref.name for pref in prefs]],
                "number_of_recipes": [number_of_recipes],
                "portion_size": [portion_size],
            }
        )

        await CombinationsAppOutput.query().insert(output_df)

async def display_recipes(response: PreselectorYearWeekResponse, col: DeltaGenerator | ModuleType) -> None:

    with st.spinner("Loading recipe information..."):
        pre_selector_recipe_info = await cached_recipe_info(
            main_recipe_ids=response.main_recipe_ids,
            year=response.year,
            week=response.week,
        )

    with st.spinner("Displaying mealkit"):
        mealkit(pre_selector_recipe_info, col)


if __name__ == "__main__":
    asyncio.run(main())
