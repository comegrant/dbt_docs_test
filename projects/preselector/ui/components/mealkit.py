from types import ModuleType
from typing import Annotated

import pandas as pd
from streamlit.delta_generator import DeltaGenerator


def badge(text: str, color: str = "#D67067", text_color: str = "white") -> str:
    return f'<span style="display: inline-block; padding: 4px 8px; background-color: {color}; color: {text_color}; border-radius: 10px; font-size: 14px; margin-bottom: 4px; border-color: gray; border-style: solid; border-width: thin;">{text}</span>'  # noqa: E501


def mealkit(recipe_information: Annotated[pd.DataFrame, "Todo"], container: DeltaGenerator | ModuleType) -> None:
    number_of_recipes = recipe_information.shape[0]
    cols = (
        container.columns(number_of_recipes)
    )

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
        col.image(row["photo_url"]) # type: ignore

        tags = " ".join(
            [badge(tag) for tag in row["taxonomies"] if tag in taxonomies_to_show],
        )
        col.markdown(tags, unsafe_allow_html=True)

        col.markdown(
            f"<span style='color: rgba(255, 255, 255, 0.5)'>Cooking time:</span> {row['cooking_time_from']} - {row['cooking_time_to']} min", # noqa: E501
            unsafe_allow_html=True,
        )

        col.write(row["recipe_name"])
        col.caption(row["main_recipe_id"])

        if "compliancy" in row:
            compliancy = row["compliancy"]
            col.caption(f"Compliancy {compliancy}")
