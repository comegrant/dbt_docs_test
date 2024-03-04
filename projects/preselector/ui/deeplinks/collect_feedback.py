from collections.abc import Awaitable
from datetime import datetime
from typing import Any

import streamlit as st
from preselector.contracts.compare_boxes import PreselectorTestChoice, recipe_information_for_ids
from pydantic import BaseModel
from streamlit_pages import set_deeplink
from ui.components.mealkit import mealkit


async def cache_awaitable(key: str, function: Awaitable) -> Any:
    if key in st.session_state:
        return st.session_state[key]

    result = await function
    st.session_state[key] = result
    return result


class ExplainSelectionState(BaseModel):
    agreement_id: int
    year: int
    week: int

    selected_create: str
    selected_recipe_ids: list[int]
    other_recipe_ids: list[int]

    selected_number_of_changes: int | None
    other_number_of_changes: int | None


async def collect_feedback(state: ExplainSelectionState) -> None:
    from ui.deeplinks.compare_week import CompareWeekState

    st.title("Why did you choose this mealkit?")

    with st.spinner("Loading recipe information..."):
        key_value_cache_key = (
            f"cached_recipe_info{state.year}_{state.week}_{state.selected_recipe_ids}"
        )
        key_value_cache_key_other = (
            f"cached_recipe_info{state.year}_{state.week}_{state.other_recipe_ids}"
        )

        choosen_recipes = await cache_awaitable(
            key_value_cache_key,
            recipe_information_for_ids(
                main_recipe_ids=state.selected_recipe_ids,
                year=state.year,
                week=state.week,
            ),
        )
        other_recipes = await cache_awaitable(
            key_value_cache_key_other,
            recipe_information_for_ids(
                main_recipe_ids=state.other_recipe_ids,
                year=state.year,
                week=state.week,
            ),
        )

    left, right = st.columns([4, 2])

    left_container = left.container(border=True)
    right_container = right.container(border=True)

    left_container.header("You chose this mealkit")
    right_container.header("The other mealkit")

    mealkit(choosen_recipes, left_container)
    mealkit(other_recipes, right_container)

    with st.form("Select mealkit"):
        st.write("### What made you choose this mealkit over the other?")

        left, right = st.columns(2)

        its_cooking_time = left.checkbox("Lower cooking time")
        its_variety = left.checkbox("More variety")
        its_interesting = left.checkbox("Recipes looked more interesting")
        its_family_friendly = left.checkbox("Recipes looked more family friendly")
        had_fewer_unwanted_ingredients = left.checkbox("Fewer unwanted ingredients")

        they_are_good = right.checkbox("I know the recipes are good")
        has_better_protines = right.checkbox("Recipes had better proteins")
        has_better_sides = right.checkbox("Recipes had better sides")
        images_looks_better = right.checkbox("Images looked more delicious")
        similar_recipes_as_last_week = right.checkbox(
            "The other recipes was in a previous week",
        )

        other_feedback = st.text_area(
            "Other Feedback",
            placeholder="Any other reason you choose this mealkit?",
        )

        submitted = st.form_submit_button(label="To next week")

    if not submitted:
        return

    with st.spinner("Saving feedback..."):
        await PreselectorTestChoice.query().insert(
            {
                "agreement_id": [state.agreement_id],
                "year": [state.year],
                "week": [state.week],
                "main_recipe_ids": [state.selected_recipe_ids],
                "compared_main_recipe_ids": [state.other_recipe_ids],
                "chosen_mealkit": [state.selected_create],
                "description": [other_feedback],
                "created_at": [datetime.now()],
                "updated_at": [datetime.now()],
                "concept_revenue": [None],
                "total_cost_of_food": [None],
                "was_lower_cooking_time": [its_cooking_time],
                "was_more_variety": [its_variety],
                "was_more_interesting": [its_interesting],
                "was_more_family_friendly": [its_family_friendly],
                "was_better_recipes": [they_are_good],
                "was_better_proteins": [has_better_protines],
                "was_better_sides": [has_better_sides],
                "was_better_images": [images_looks_better],
                "was_fewer_unwanted_ingredients": [had_fewer_unwanted_ingredients],
                "had_recipes_last_week": [similar_recipes_as_last_week],
                "compared_number_of_recipes_to_change": [state.other_number_of_changes],
                "number_of_recipes_to_change": [state.selected_number_of_changes],
            },
        )

    set_deeplink(
        CompareWeekState(
            agreement_id=state.agreement_id,
            year=state.year,
            week=state.week + 1,
        ),
    )
