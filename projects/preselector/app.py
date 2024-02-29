import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from random import choice, seed
from time import sleep
from typing import TypeVar

import pandas as pd
import streamlit as st
from data_contracts.sources import SqlServerConfig, adb, data_science_data_lake
from preselector.contracts.compare_boxes import PreselectorTestChoice, RecipeInformation
from preselector.data.models.customer import PreselectorCustomer
from preselector.main import RunConfig, run_preselector_for
from preselector.mealselector_api import run_mealselector
from preselector.utils.constants import FLEX_PRODUCT_TYPE_ID
from pydantic import BaseModel
from streamlit.delta_generator import DeltaGenerator

logger = logging.getLogger(__name__)


preselector_ab_test_folder = data_science_data_lake.directory("preselector/ab-test")


T = TypeVar("T", bound=BaseModel)


class SetupState(BaseModel):
    pass


class CompareWeekState(BaseModel):
    agreement_id: int
    year: int
    week: int


class ExplainSelectionState(BaseModel):
    agreement_id: int
    year: int
    week: int

    selected_create: str
    selected_recipe_ids: list[int]
    other_recipe_ids: list[int]

    selected_number_of_changes: int | None
    other_number_of_changes: int | None


class ShowChoicesState(BaseModel):
    agreement_id: int
    year_weeks: list[tuple[int, int]]


def has_deeplink() -> bool:
    return st.query_params.get("state") is not None


def set_deeplink(state: BaseModel):
    json_state = state.model_dump_json()
    state_name = state.__class__.__name__

    st.query_params.clear()
    st.query_params["state"] = state_name
    st.query_params["state_json"] = json_state

    with st.spinner(f"Routing to {state_name}..."):
        # Needed to make sure the parameters are set correctly
        sleep(1)

    st.rerun()


def read_deeplink(state: type[T]) -> T | None:
    state_name = state.__name__
    if st.query_params.get("state") != state_name:
        return None

    state_json = st.query_params.get("state_json", "")
    return state.model_validate_json(state_json)


def read_cache(key: str):
    return st.session_state.get(key, None)


def set_cache(key: str, value):
    st.session_state[key] = value


async def load_recipe_information(
    main_recipe_id: list[int],
    year: int,
    week: int,
    db_config: SqlServerConfig,
) -> pd.DataFrame:
    key_value_cache_key = f"load_recipe_information{main_recipe_id}_{year}_{week}"
    cache_value = read_cache(key_value_cache_key)
    if cache_value is not None:
        return cache_value

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
      WHERE rec.main_recipe_id IN ({', '.join([str(x) for x in main_recipe_id])}) AND rec.recipes_year = {year} AND rec.recipes_week= {week}) as recipes
WHERE recipes.nr = 1"""
    data = await db_config.fetch(recipe_sql).to_pandas()
    set_cache(key_value_cache_key, data)
    return data


async def load_customer_information(
    agreement_id: int,
    db_config: SqlServerConfig,
) -> PreselectorCustomer:
    key_value_cache_key = f"load_customer_information{agreement_id}"
    cache_value = read_cache(key_value_cache_key)
    if cache_value is not None:
        return cache_value

    concept_preference_type_id = "009cf63e-6e84-446c-9ce4-afdbb6bb9687"
    taste_preference_type_id = "4c679266-7dc0-4a8e-b72d-e9bb8dadc7eb"
    mealbox_product_type_id = "2f163d69-8ac1-6e0c-8793-ff0000804eb3"

    customer_sql = f"""

declare @agreement_id integer = '{agreement_id}';
declare @concept_preference_type_id uniqueidentifier = '{concept_preference_type_id}';
declare @taste_preference_type_id uniqueidentifier = '{taste_preference_type_id}';
DECLARE @PRODUCT_TYPE_ID_MEALBOX uniqueidentifier = '{mealbox_product_type_id}';

WITH concept_preferences AS (
    SELECT
        bap.agreement_id,
        STRING_AGG(convert(nvarchar(36), bap.preference_id), ', ') as preference_ids,
        STRING_AGG(pref.name, ', ') as preferences
    from cms.billing_agreement_preference bap
        JOIN cms.preference pref on pref.preference_id = bap.preference_id
        WHERE pref.preference_type_id = @concept_preference_type_id
    GROUP BY bap.agreement_id
),

taste_preferences AS (
    SELECT
        bap.agreement_id,
        STRING_AGG(convert(nvarchar(36), bap.preference_id), ', ') as preference_ids,
        STRING_AGG(pref.name, ', ') as preferences
    from cms.billing_agreement_preference bap
        JOIN cms.preference pref on pref.preference_id = bap.preference_id
        WHERE pref.preference_type_id = @taste_preference_type_id
    GROUP BY bap.agreement_id
),

default_sub AS (
    SELECT
        MENU_VARIATION_EXT_ID as variation_id,
        MAX(MENU_NUMBER_DAYS) as number_of_recipes,
        MAX(p.PORTION_SIZE) as portion_size
    FROM pim.MENU_VARIATIONS mv
    INNER JOIN pim.PORTIONS p on mv.PORTION_ID = p.PORTION_ID
    GROUP BY MENU_VARIATION_EXT_ID
)


SELECT ba.agreement_id
        , ba.company_id
        , ba.status
        , taste_pref.preference_ids taste_preference_ids
        , taste_pref.preferences taste_preference_names
        , concept_pref.preference_ids concept_preference_id
        , concept_pref.preferences concept_preference_names
        , bap.subscribed_product_variation_id
        , bap.quantity
        , ds.number_of_recipes
        , ds.portion_size
    FROM cms.billing_agreement ba
    LEFT JOIN taste_preferences taste_pref on taste_pref.agreement_id = ba.agreement_id
    LEFT JOIN concept_preferences concept_pref on concept_pref.agreement_id = ba.agreement_id
    LEFT JOIN cms.billing_agreement_basket bb on bb.agreement_id = ba.agreement_id
    LEFT JOIN cms.billing_agreement_basket_product bap on bap.billing_agreement_basket_id = bb.id
    LEFT JOIN default_sub ds on ds.variation_id = bap.subscribed_product_variation_id
    INNER JOIN product_layer.product_variation pv ON pv.id = bap.subscribed_product_variation_id
    INNER JOIN product_layer.product p ON p.id = pv.product_id
    WHERE
        p.product_type_id = @PRODUCT_TYPE_ID_MEALBOX
        AND ba.agreement_id = @agreement_id
    """

    df = await db_config.fetch(customer_sql).to_pandas()
    df["taste_preference_ids"] = df["taste_preference_ids"].apply(
        lambda x: x.lower().split(", ") if isinstance(x, str) else [],
    )
    customer = PreselectorCustomer(**df.to_dict(orient="records")[0])
    set_cache(key_value_cache_key, customer)
    return customer


async def load_recommendations(
    agreement_id: int,
    year: int,
    week: int,
    db_config: SqlServerConfig,
) -> pd.DataFrame:
    key_value_cache_key = f"load_recommendations{agreement_id}_{year}_{week}"
    cache_value = read_cache(key_value_cache_key)
    if cache_value is not None:
        return cache_value

    recommendation_sql = f"""declare @agreement_id int = '{agreement_id}', @week int = '{week}', @year int = '{year}';

SELECT [agreement_id]
      ,[run_timestamp]
      ,[product_id]
      ,[order_of_relevance]
      ,[order_of_relevance_cluster]
      ,[cluster]
  FROM [ml_output].[latest_recommendations]
  WHERE agreement_id = @agreement_id
  AND week = @week
  AND year = @year"""

    df = await db_config.fetch(recommendation_sql).to_pandas()
    set_cache(key_value_cache_key, df)
    return df


async def load_menu_for(
    company_id: str,
    year: int,
    week: int,
    db_config: SqlServerConfig,
) -> pd.DataFrame:
    key_value_cache_key = f"load_menu_for{company_id}_{year}_{week}.parquet"
    cache = preselector_ab_test_folder.parquet_at(key_value_cache_key)

    try:
        return await cache.to_pandas()
    except Exception:
        pass

    df_menu_with_preferences = await db_config.fetch(
        f"""declare @company uniqueidentifier = '{company_id}', @week int = '{week}', @year int = '{year}';

with recipe_preferences AS (
    SELECT
        recipe_id,
        portion_id,
        STRING_AGG(convert(nvarchar(36), preference_id), ', ') as preference_ids
        FROM pim.recipes_category_preferences_mapping
        GROUP BY recipe_id, portion_id
    )


SELECT
    wm.menu_week,
    wm.menu_year,
    mv.menu_variation_ext_id as variation_id,
    r.main_recipe_id as main_recipe_id,
    rcpm.recipe_id,
    rcpm.portion_id,
    rcpm.preference_ids,
    rp.recipe_portion_id,
    mr.menu_recipe_order
  FROM pim.weekly_menus wm
  JOIN pim.menus m on m.weekly_menus_id = wm.weekly_menus_id
  JOIN pim.menu_variations mv on mv.menu_id = m.menu_id
  JOIN pim.menu_recipes mr on mr.menu_id = m.menu_id
  JOIN pim.recipes r on r.recipe_id = mr.RECIPE_ID
  INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
  JOIN recipe_preferences rcpm on rcpm.recipe_id = mr.recipe_id and rcpm.portion_id = mv.portion_id
  where m.RECIPE_STATE = 1
  AND wm.menu_week = @week AND wm.menu_year = @year AND wm.company_id = @company""",
    ).to_pandas()

    df_products = await db_config.fetch(
        f"""
    declare @company uniqueidentifier = '{company_id}';

SELECT
    p.id AS product_id
,	p.name AS product_name
,	pt.product_type_name AS product_type
,       pt.product_type_id
,	pv.id AS variation_id
,	pv.sku AS variation_sku
,	pvc.name AS variation_name
,	pvc.company_id AS company_id
,	ISNULL(pvav.attribute_value, pvat.default_value) AS variation_meals
,	ISNULL(pvav2.attribute_value, pvat2.default_value) AS variation_portions
,	ISNULL(pvav4.attribute_value, pvat4.default_value) AS variation_price
,	ROUND(CAST(ISNULL(pvav4.attribute_value, pvat4.default_value) AS FLOAT) * (1.0 + (CAST(ISNULL(pvav5.attribute_value, pvat5.default_value) AS FLOAT) / 100)), 2) AS variation_price_incl_vat
,	ISNULL(pvav5.attribute_value, pvat5.default_value) AS variation_vat
FROM product_layer.product p
INNER JOIN product_layer.product_type pt ON pt.product_type_id = p.product_type_id
INNER JOIN product_layer.product_variation pv ON pv.product_id = p.id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id
INNER JOIN cms.company c ON c.id = pvc.company_id

-- MEALS
LEFT JOIN product_layer.product_variation_attribute_template pvat ON pvat.product_type_id = p.product_type_id AND pvat.attribute_name = 'Meals'
LEFT JOIN product_layer.product_variation_attribute_value pvav ON pvav.attribute_id = pvat.attribute_id AND pvav.variation_id = pvc.variation_id AND pvav.company_id = pvc.company_id

-- PORTIONS
LEFT JOIN product_layer.product_variation_attribute_template pvat2 ON pvat2.product_type_id = p.product_type_id AND pvat2.attribute_name = 'Portions'
LEFT JOIN product_layer.product_variation_attribute_value pvav2 ON pvav2.attribute_id = pvat2.attribute_id AND pvav2.variation_id = pvc.variation_id AND pvav2.company_id = pvc.company_id

-- PRICE
LEFT JOIN product_layer.product_variation_attribute_template pvat4 ON pvat4.product_type_id = p.product_type_id AND pvat4.attribute_name = 'PRICE'
LEFT JOIN product_layer.product_variation_attribute_value pvav4 ON pvav4.attribute_id = pvat4.attribute_id AND pvav4.variation_id = pvc.variation_id AND pvav4.company_id = pvc.company_id

-- VAT
LEFT JOIN product_layer.product_variation_attribute_template pvat5 ON pvat5.product_type_id = p.product_type_id AND pvat5.attribute_name = 'VAT'
LEFT JOIN product_layer.product_variation_attribute_value pvav5 ON pvav5.attribute_id = pvat5.attribute_id AND pvav5.variation_id = pvc.variation_id AND pvav5.company_id = pvc.company_id

WHERE pvc.company_id=@company """,
    ).to_pandas()

    # Merge together information to menu product with preferences
    df_menu_products_with_preferences = df_menu_with_preferences.merge(
        df_products,
        on="variation_id",
        how="left",
    )

    # Replace nan values for no preferences with empty bracked []
    df_menu_products_with_preferences = df_menu_products_with_preferences.assign(
        preference_ids=df_menu_products_with_preferences["preference_ids"].apply(
            lambda x: x.lower().split(", ") if isinstance(x, str) else [],
        ),
    )
    df_menu_products_with_preferences = df_menu_products_with_preferences.astype(
        {"variation_price": float},
    )

    await cache.write_pandas(df_menu_products_with_preferences)

    return df_menu_products_with_preferences


def badge(text: str, color: str = "#D67067", text_color: str = "white") -> str:
    return f'<span style="display: inline-block; padding: 4px 8px; background-color: {color}; color: {text_color}; border-radius: 10px; font-size: 14px; margin-bottom: 4px; border-color: gray; border-style: solid; border-width: thin;">{text}</span>'


def mealkit(recipe_information: pd.DataFrame, container: DeltaGenerator):
    number_of_recipes = recipe_information.shape[0]
    cols = container.columns(3) if number_of_recipes == 5 else container.columns(number_of_recipes)

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
        col = cols[int(index) % len(cols)]
        col.image(row["photo_url"])

        tags = " ".join(
            [badge(tag) for tag in row["taxonomies"] if tag in taxonomies_to_show],
        )
        col.markdown(tags, unsafe_allow_html=True)

        col.markdown(
            f"<span style='color: rgba(255, 255, 255, 0.5)'>Cooking time:</span> {row['cooking_time_from']} - {row['cooking_time_to']} min",
            unsafe_allow_html=True,
        )

        col.write(row["recipe_name"])


async def run_comparison_for(
    year: int,
    week: int,
    customer: PreselectorCustomer,
    run_config: RunConfig,
):
    st.write(
        f"Creating a menu with {customer.number_of_recipes} recipes and {customer.portion_size} portions",
    )

    def to_next_week():
        set_deeplink(
            CompareWeekState(
                agreement_id=customer.agreement_id,
                year=year,
                week=week + 1,
            ),
        )


    def view_results():
        current_date = date.today() + timedelta(weeks=1)
        current_year = current_date.isocalendar()[0]
        current_week = current_date.isocalendar()[1]

        year_weeks = []

        while year != current_year or week != current_week:
            year_weeks.append((current_year, current_week))
            current_date = current_date + timedelta(weeks=1)
            current_year = current_date.isocalendar()[0]
            current_week = current_date.isocalendar()[1]

        st.info("Showing results.")

        set_deeplink(
            ShowChoicesState(agreement_id=customer.agreement_id, year_weeks=year_weeks),
        )

    if st.button("Skip weeks and view results"):
        view_results()
        return

    with st.spinner("Loading recommendation data..."):
        recommendations = await load_recommendations(
            agreement_id=customer.agreement_id,
            year=year,
            week=week,
            db_config=adb,
        )

    if recommendations.empty:
        current_date = date.today() + timedelta(weeks=1)
        current_year = current_date.isocalendar()[0]
        current_week = current_date.isocalendar()[1]

        year_weeks = []

        while year != current_year or week != current_week:
            year_weeks.append((current_year, current_week))
            current_date = current_date + timedelta(weeks=1)
            current_year = current_date.isocalendar()[0]
            current_week = current_date.isocalendar()[1]

        if len(year_weeks) == 0:
            to_next_week()

        st.info("No recommendations found for this week. Showing results.")

        set_deeplink(
            ShowChoicesState(agreement_id=customer.agreement_id, year_weeks=year_weeks),
        )
        return

    with st.spinner("Loading Meny data..."):
        menu = await load_menu_for(customer.company_id, year, week, adb)

    df_flex_products = menu[menu["product_type_id"] == FLEX_PRODUCT_TYPE_ID.upper()].explode(
        "main_recipe_id",
    )

    with st.spinner("Running preselector..."):
        preselector_result = run_preselector_for(
            customer=customer,
            run_config=run_config,
            df_flex_products=df_flex_products,
            df_recommendations=recommendations,
        )


    if isinstance(preselector_result, Exception):
        st.error(f"An error occurred when running the pre-selector: {preselector_result}")
        to_next_week()
        return

    if len(preselector_result.main_recipe_ids) == 0:
        st.error("No recipes found for the preselector.")
        to_next_week()
        return

    with st.spinner("Fetching chef's selection..."):
        cache_key = f"mealselector_{customer.model_dump_json()}_{year}_{week}"

        mealselector_result = read_cache(cache_key)

        if not mealselector_result:
            mealselector_result = await run_mealselector(customer, year, week, menu)
            set_cache(cache_key, mealselector_result)

    if isinstance(mealselector_result, Exception) or not mealselector_result:
        st.error(f"An error occurred when fetching the chef-selection: {mealselector_result}")
        to_next_week()
        return

    if len(mealselector_result) == 0:
        st.info("No recipes found for the mealselector")
        to_next_week()
        return


    with st.spinner("Loading recipe information..."):
        pre_selector_recipe_info = await load_recipe_information(
            main_recipe_id=preselector_result.main_recipe_ids,
            year=year,
            week=week,
            db_config=adb,
        )
        meal_selector_recipe_info = await load_recipe_information(
            main_recipe_id=mealselector_result,
            year=year,
            week=week,
            db_config=adb,
        )

        pre_selector_recipe_info = await RecipeInformation.process_input(
            pre_selector_recipe_info,
        ).to_pandas()
        other_recipe_info = await RecipeInformation.process_input(
            meal_selector_recipe_info,
        ).to_pandas()

    choices = {"P": pre_selector_recipe_info, "M": other_recipe_info}

    # Needed to make sure the random selection is the same when navigating
    seed(100 * year + week + customer.agreement_id * 1000)
    first_choice = choice(list(choices.keys()))

    if first_choice == "P":
        infos = [
            ("pre-selector", pre_selector_recipe_info),
            ("chef-selection", other_recipe_info),
        ]
    else:
        infos = [
            ("chef-selection", other_recipe_info),
            ("pre-selector", pre_selector_recipe_info),
        ]

    left, right = st.columns(2)

    left = left.container(border=True)
    left.header("Mealkit A")
    mealkit(infos[0][1], left)

    right = right.container(border=True)
    right.header("Mealkit B")
    mealkit(infos[1][1], right)

    left.markdown("---")
    right.markdown("---")

    change_a = left.radio(
        "Optional - How many would you change in A?",
        options=list(range(len(infos[0][1]) + 1)),
        index=None,
    )
    change_b = right.radio(
        "Optional - How many would you change in B?",
        options=list(range(len(infos[0][1]) + 1)),
        index=None,
    )

    # if change_a is None or change_b is None:

    # if change_a == change_b:
    if left.button("I choose mealkit A"):
        set_deeplink(
            ExplainSelectionState(
                agreement_id=customer.agreement_id,
                year=year,
                week=week,
                selected_create=infos[0][0],
                selected_recipe_ids=infos[0][1]["main_recipe_id"].tolist(),
                other_recipe_ids=infos[1][1]["main_recipe_id"].tolist(),
                selected_number_of_changes=int(change_a) if change_a else None,
                other_number_of_changes=int(change_b) if change_b else None,
            ),
        )

    if right.button("I choose mealkit B"):
        set_deeplink(
            ExplainSelectionState(
                agreement_id=customer.agreement_id,
                year=year,
                week=week,
                selected_create=infos[1][0],
                selected_recipe_ids=infos[1][1]["main_recipe_id"].tolist(),
                other_recipe_ids=infos[0][1]["main_recipe_id"].tolist(),
                selected_number_of_changes=int(change_b) if change_b else None,
                other_number_of_changes=int(change_a) if change_a else None,
            ),
        )
    #
    #     set_deeplink(
    #         ExplainSelectionState(
    #


@dataclass
class DeeplinkRouter:
    initial_state: BaseModel
    routes: dict[type[BaseModel], Callable[[BaseModel], Awaitable[None]]]

    async def run(self):
        if not has_deeplink():
            await self.routes[self.initial_state.__class__](self.initial_state)
        else:
            for state, handler in self.routes.items():
                state_instance = read_deeplink(state)
                if state_instance:
                    await handler(state_instance)
                    break

    def add_route(self, state: type[T], handler: Callable[[T], Awaitable[None]]):
        self.routes[state] = handler


async def compare_week(state: CompareWeekState):
    st.title(f"Compare for year: {state.year} - week: {state.week}")

    config = RunConfig()

    with st.spinner("Loading Customer data..."):
        customer = await load_customer_information(
            agreement_id=state.agreement_id,
            db_config=adb,
        )

    await run_comparison_for(state.year, state.week, customer, config)


async def collect_feedback(state: ExplainSelectionState):
    st.title("Why did you choose this mealkit?")

    with st.spinner("Loading recipe information..."):
        recipes = await load_recipe_information(
            main_recipe_id=state.selected_recipe_ids,
            year=state.year,
            week=state.week,
            db_config=adb,
        )
        other_recipes = await load_recipe_information(
            main_recipe_id=state.other_recipe_ids,
            year=state.year,
            week=state.week,
            db_config=adb,
        )

    choosen_recipes = await RecipeInformation.process_input(recipes).to_pandas()
    other_recipes = await RecipeInformation.process_input(other_recipes).to_pandas()

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
        similar_recipes_as_last_week = right.checkbox("The other recipes was in a previous week")

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
        )
    )


async def show_choices(state: ShowChoicesState):
    entites = defaultdict(list)

    for year, week in state.year_weeks:
        entites["year"].append(year)
        entites["week"].append(week)
        entites["agreement_id"].append(state.agreement_id)

    with st.spinner("Loading choices..."):
        choices = await PreselectorTestChoice.query().features_for(entites).to_pandas()

    counts = choices["chosen_mealkit"].value_counts()
    preselector_count = counts.get("pre-selector", 0)
    menu_team_count = counts.get("chef-selection", 0)

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
            "had_recipes_last_week"
        ]
    ].sum()
    why_stats.sort_values(ascending=False, inplace=True)

    #

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
            recipes = await load_recipe_information(
                main_recipe_id=row["main_recipe_ids"],
                year=row["year"],
                week=row["week"],
                db_config=adb,
            )

        recipes = await RecipeInformation.process_input(recipes).to_pandas()

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
            recipes = await load_recipe_information(
                main_recipe_id=row["compared_main_recipe_ids"],
                year=row["year"],
                week=row["week"],
                db_config=adb,
            )

        recipes = await RecipeInformation.process_input(recipes).to_pandas()
        mealkit(recipes, expander)


async def router():
    st.set_page_config(layout="wide")

    router = DeeplinkRouter(initial_state=SetupState(), routes={})

    router.add_route(SetupState, test_preselector)
    router.add_route(CompareWeekState, compare_week)
    router.add_route(ExplainSelectionState, collect_feedback)
    router.add_route(ShowChoicesState, show_choices)

    await router.run()


async def test_preselector(state: SetupState):
    st.title("Preselector A/B Test")
    with st.form("Preselector"):
        agreement_id = st.number_input("Agreement ID", step=1, min_value=0)

        submitted = st.form_submit_button(label="Start comparison")

    st.info(
        """Unsure what the agreement ID is?

Go to "Verv en venn", and find the digits at the end of the URL.

E.g. `https://godtlevert.no/verv-en-venn/?rk=MATS12345`

Here is the agreement ID `12345`.

Or you can find a receipt from a mealbox and you will see the agreement ID or "Kundenummer" on the top.""",
    )

    if not submitted or agreement_id is None:
        return

    current_date = date.today() + timedelta(weeks=1)
    year = current_date.isocalendar()[0]
    week = current_date.isocalendar()[1]

    set_deeplink(CompareWeekState(agreement_id=int(agreement_id), year=year, week=week))


if __name__ == "__main__":
    asyncio.run(router())
