import logging
from collections.abc import Awaitable
from types import ModuleType
from typing import Annotated, Any

import pandas as pd
import streamlit as st
from aligned import Int32, String, feature_view
from aligned.compiler.feature_factory import List
from data_contracts.sources import SqlServerConfig, adb, data_science_data_lake
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


@feature_view(
    name="recipe_information",
    source=data_science_data_lake.delta_at("recipe_information.delta"),
)
class RecipeInformation:
    main_recipe_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    cooking_time_from = Int32()
    cooking_time_to = Int32()

    average_cooking_time = (cooking_time_from + cooking_time_to) / 2

    recipe_photo = String()
    recipe_name = String()
    taxonomie_names = String()

    taxonomies = taxonomie_names.transform_pandas(
        lambda x, store: x["taxonomie_names"].str.split(", ").apply(lambda x: list(set(x))),
        as_dtype=List(String()),
    )
    photo_url = recipe_photo.prepend("https://pimimages.azureedge.net/images/resized/")


async def recipe_information_for_ids(
    main_recipe_ids: list[int],
    year: int,
    week: int,
    database: SqlServerConfig | None = None,
) -> Annotated[pd.DataFrame, RecipeInformation]:
    if not main_recipe_ids:
        return pd.DataFrame()

    recipe_sql = f"""
WITH taxonomies AS (
    SELECT
        rt.RECIPE_ID as recipe_id,
        STRING_AGG(tt.TAXONOMIES_NAME, ', ') as taxonomie_names
    FROM pim.TAXONOMIES_TRANSLATIONS tt
    INNER JOIN pim.RECIPES_TAXONOMIES rt on rt.TAXONOMIES_ID = tt.TAXONOMIES_ID
    INNER JOIN pim.taxonomies t ON t.taxonomies_id = tt.TAXONOMIES_ID
    WHERE t.taxonomy_type IN ('1', '11', '12')
    GROUP BY rt.RECIPE_ID
)

SELECT *
FROM (SELECT rec.recipe_id,
             rec.main_recipe_id,
             rec.recipes_year as year,
             rec.recipes_week as week,
             rm.RECIPE_PHOTO as recipe_photo,
             rm.COOKING_TIME_FROM as cooking_time_from,
             rm.COOKING_TIME_TO as cooking_time_to,
             rmt.recipe_name,
             tx.taxonomie_names,
             ROW_NUMBER() over (PARTITION BY main_recipe_id ORDER BY rmt.language_id) as nr
      FROM pim.recipes rec
        INNER JOIN pim.recipes_metadata rm ON rec.recipe_metadata_id = rm.RECIPE_METADATA_ID
        INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = rec.recipe_metadata_id
        INNER JOIN taxonomies tx ON tx.recipe_id = rec.recipe_id
      WHERE
        rec.main_recipe_id IN ({", ".join([str(x) for x in main_recipe_ids])})
        AND rec.recipes_year = {year}
        AND rec.recipes_week = {week}) as recipes
WHERE recipes.nr = 1"""

    if not database:
        database = adb

    query = database.fetch(recipe_sql)

    return await RecipeInformation.query().using_source(query).all().to_pandas()


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
