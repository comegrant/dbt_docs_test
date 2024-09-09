from collections import defaultdict

import pandas as pd
import streamlit as st
from data_contracts.preselector.store import PreselectorTestChoice
from preselector.contracts.compare_boxes import recipe_information_for_ids
from pydantic import BaseModel
from ui.components.mealkit import badge, mealkit


class ShowChoicesState(BaseModel):
    agreement_id: int
    year_weeks: list[tuple[int, int]]


async def show_choices(state: ShowChoicesState) -> None:
    entites = defaultdict(list)

    for year, week in state.year_weeks:
        entites["year"].append(year)
        entites["week"].append(week)
        entites["agreement_id"].append(state.agreement_id)

    if not entites["year"]:
        st.title("Your choices")
        st.warning("Found no weeks to show choices for.")
        return

    with st.spinner("Loading choices..."):
        choices = (await PreselectorTestChoice.query().features_for(entites).to_polars()).to_pandas()

    counts = choices["chosen_mealkit"].value_counts()
    preselector_count = counts.get("pre-selector", 0)
    menu_team_count = counts.get("chef-selection", 0)

    assert isinstance(preselector_count, int)
    assert isinstance(menu_team_count, int)

    if preselector_count > menu_team_count:
        st.title("**You have chosen the** _Pre-selector_ 🍽️ **most often**")
    else:
        st.title("**You have chosen the** _Chef-selection_ 🧑‍🍳 **most often**")

    why_stats: pd.Series = choices[
        [
            "was_fewer_unwanted_ingredients",
            "was_lower_cooking_time",
            "was_more_variety",
            "was_more_interesting",
            "was_more_family_friendly",
            "was_better_recipes",
            "was_better_proteins",
            "was_better_sides",
            "was_better_images",
            "had_recipes_last_week",
        ]
    ].sum()
    why_stats.sort_values(ascending=False, inplace=True)


    columns = st.columns(2)
    columns[0].metric(
        "Pre-selector mealkits 🍽️ ",
        counts.get("pre-selector", 0),
    )
    columns[1].metric(
        "Chef-selection mealkits 🧑‍🍳",
        counts.get("chef-selection", 0),
    )

    st.write("### Reasons for choosing one over the other")
    reasons = st.columns(3)

    for index, values in enumerate(why_stats[why_stats > 0].items()):
        name, value = values

        col = reasons[index % len(reasons)]
        col.metric(name.replace("_", " "), f"{value} time(s)")

    columns = st.columns(2)

    for index, row in choices.iterrows():
        if row["chosen_mealkit"] is None or pd.isna(row["chosen_mealkit"]):
            continue

        col = columns[index % len(columns)]

        with st.spinner("Loading recipe information..."):
            recipes = await recipe_information_for_ids(
                main_recipe_ids=row["main_recipe_ids"].tolist(),
                year=row["year"],
                week=row["week"],
            )

        col.header(f"Year: {row['year']} - Week: {row['week']}")
        mealkit(recipes, col.container(border=True))

        badge_html = badge(
            row["chosen_mealkit"],
            color="#D67067" if row["chosen_mealkit"] == "chef-selection" else "#07005e",
            text_color="white",
        )
        col.markdown(
            f"<span style='color: rgba(255, 255, 255, 0.5)'>Mealkit created by:</span> {badge_html}",
            unsafe_allow_html=True,
        )

        if row["description"]:
            col.markdown(
                f"<span style='color: rgba(255, 255, 255, 0.5)'>Chosen because:</span> **{row['description']}**",
                unsafe_allow_html=True,
            )

        expander = col.expander("Show Alternative", expanded=False)
        with st.spinner("Loading recipe information..."):
            recipes = await recipe_information_for_ids(
                main_recipe_ids=row["compared_main_recipe_ids"].tolist(),
                year=row["year"],
                week=row["week"],
            )

        mealkit(recipes, expander)
