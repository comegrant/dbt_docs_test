import logging
from collections.abc import Awaitable
from types import ModuleType
from typing import Annotated, Any

import pandas as pd
import streamlit as st
from data_contracts.recipe import RecipeFeatures
from streamlit.delta_generator import DeltaGenerator

logger = logging.getLogger(__name__)


def badge(text: str, color: str = "#D67067", text_color: str = "white") -> str:
    return f'<span style="display: inline-block; padding: 4px 8px; background-color: {color}; color: {text_color}; border-radius: 10px; font-size: 14px; margin-bottom: 4px; border-color: gray; border-style: solid; border-width: thin;">{text}</span>'  # noqa: E501


def mealkit(recipe_information: Annotated[pd.DataFrame, "Todo"], container: DeltaGenerator | ModuleType) -> None:
    number_of_recipes = recipe_information.shape[0]
    cols = container.columns(number_of_recipes)

    taxonomies_to_show = [
        "Vegetarisk",
        "Vegan",
        "Laktosefri",
        "Glutenfri",
        "Roede",
        "Vegetar",
        "Godt og rimelig",
    ]

    for index, row in recipe_information.iterrows():
        assert isinstance(index, int)

        col = cols[int(index)]
        col.image(row["photo_url"])  # type: ignore

        tags = " ".join(
            [badge(tag) for tag in row["taxonomies"] if tag in taxonomies_to_show],
        )
        col.markdown(tags, unsafe_allow_html=True)

        col.markdown(
            f"<span style='color: rgba(255, 255, 255, 0.5)'>Cooking time:</span> {row['cooking_time_from']} - {row['cooking_time_to']} min",  # noqa: E501
            unsafe_allow_html=True,
        )

        col.write(row["recipe_name"])
        col.caption(row["main_recipe_id"])

        if "compliancy" in row:
            compliancy = row["compliancy"]
            col.caption(f"Compliancy {compliancy}")


async def recipe_information_for_ids(
    main_recipe_ids: list[int],
    year: int,
    week: int,
) -> Annotated[pd.DataFrame, RecipeFeatures]:
    import polars as pl

    if not main_recipe_ids:
        return pd.DataFrame()

    return (
        await RecipeFeatures.query()
        .filter(
            pl.col(RecipeFeatures().main_recipe_id.name).is_in(main_recipe_ids)
            & (pl.col(RecipeFeatures().year.name) == year)
            & (pl.col(RecipeFeatures().week.name) == week)
        )
        .to_pandas()
    )


async def cached_recipe_info(
    main_recipe_ids: list[int],
    year: int,
    week: int,
) -> pd.DataFrame:
    key_value_cache_key = f"cached_recipe_info{year}_{week}_{main_recipe_ids}"

    return await cache_awaitable(
        key_value_cache_key,
        recipe_information_for_ids(main_recipe_ids, year, week),
    )


async def cache_awaitable(key: str, function: Awaitable) -> Any:  # noqa: ANN401
    if key in st.session_state:
        return st.session_state[key]

    result = await function
    st.session_state[key] = result
    return result
