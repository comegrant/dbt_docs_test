from collections.abc import Awaitable
from datetime import date, timedelta
from random import choice, seed
from typing import Any

import pandas as pd
import polars as pl
import streamlit as st
from data_contracts.preselector.menu import PreselectorYearWeekMenu
from preselector.contracts.compare_boxes import (
    SqlServerConfig,
    customer_information,
    data_science_data_lake,
    recipe_information_for_ids,
)
from preselector.data.models.customer import PreselectorCustomer
from preselector.main import run_preselector_for_request
from preselector.mealselector_api import run_mealselector
from preselector.process_stream import convert_concepts_to_attributes, load_cache
from preselector.schemas.batch_request import GenerateMealkitRequest, NegativePreference, YearWeek
from preselector.store import preselector_store
from pydantic import BaseModel
from streamlit_pages import set_deeplink
from ui.components.mealkit import mealkit
from ui.deeplinks.show_results import ShowChoicesState

# from ui.deeplinks.collect_feedback import ExplainSelectionState

preselector_ab_test_folder = data_science_data_lake.directory("preselector/ab-test")


def read_cache(key: str) -> Any | None:  # noqa: ANN401
    return st.session_state.get(key, None)


def set_cache(key: str, value: Any) -> None:  # noqa: ANN401
    st.session_state[key] = value


async def load_recommendations(agreement_id: int, year: int, week: int) -> pd.DataFrame:
    import polars as pl
    from data_contracts.recommendations.store import recommendation_feature_contracts

    recommendations_key = f"recommendations_{agreement_id}"
    cache_value = read_cache(recommendations_key)
    if cache_value is not None:
        return cache_value.filter(pl.col("week") == week, pl.col("year") == year).to_pandas()

    store = recommendation_feature_contracts()

    preds = await store.model("rec_engine").all_predictions().to_lazy_polars()
    df = (
        preds.filter(
            pl.col("agreement_id") == agreement_id,
        )
        .sort("predicted_at", descending=True)
        .unique(["agreement_id", "week", "year", "product_id"], keep="first")
        .collect()
    )

    set_cache(recommendations_key, df)
    return df.filter(
        pl.col("week") == week,
        pl.col("year") == year,
    ).to_pandas()


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

SELECT
    wm.menu_week,
    wm.menu_year,
    mv.menu_variation_ext_id as variation_id,
    r.main_recipe_id as main_recipe_id,
    r.recipe_id,
    mv.portion_id,
    rp.recipe_portion_id,
    mr.menu_recipe_order
  FROM pim.weekly_menus wm
  JOIN pim.menus m on m.weekly_menus_id = wm.weekly_menus_id
  JOIN pim.menu_variations mv on mv.menu_id = m.menu_id
  JOIN pim.menu_recipes mr on mr.menu_id = m.menu_id
  JOIN pim.recipes r on r.recipe_id = mr.RECIPE_ID
  INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
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
,	ROUND(CAST(ISNULL(pvav4.attribute_value, pvat4.default_value) AS FLOAT) * (
            1.0 + (CAST(ISNULL(pvav5.attribute_value, pvat5.default_value) AS FLOAT) / 100)), 2
        ) AS variation_price_incl_vat
,	ISNULL(pvav5.attribute_value, pvat5.default_value) AS variation_vat
FROM product_layer.product p
INNER JOIN product_layer.product_type pt ON pt.product_type_id = p.product_type_id
INNER JOIN product_layer.product_variation pv ON pv.product_id = p.id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id
INNER JOIN cms.company c ON c.id = pvc.company_id

-- MEALS
LEFT JOIN product_layer.product_variation_attribute_template pvat
    ON pvat.product_type_id = p.product_type_id AND pvat.attribute_name = 'Meals'
LEFT JOIN product_layer.product_variation_attribute_value pvav
    ON pvav.attribute_id = pvat.attribute_id
    AND pvav.variation_id = pvc.variation_id AND pvav.company_id = pvc.company_id

-- PORTIONS
LEFT JOIN product_layer.product_variation_attribute_template pvat2
    ON pvat2.product_type_id = p.product_type_id AND pvat2.attribute_name = 'Portions'
LEFT JOIN product_layer.product_variation_attribute_value pvav2
    ON pvav2.attribute_id = pvat2.attribute_id
    AND pvav2.variation_id = pvc.variation_id AND pvav2.company_id = pvc.company_id

-- PRICE
LEFT JOIN product_layer.product_variation_attribute_template pvat4
    ON pvat4.product_type_id = p.product_type_id AND pvat4.attribute_name = 'PRICE'
LEFT JOIN product_layer.product_variation_attribute_value pvav4
    ON pvav4.attribute_id = pvat4.attribute_id AND pvav4.variation_id = pvc.variation_id
        AND pvav4.company_id = pvc.company_id

-- VAT
LEFT JOIN product_layer.product_variation_attribute_template pvat5
    ON pvat5.product_type_id = p.product_type_id AND pvat5.attribute_name = 'VAT'
LEFT JOIN product_layer.product_variation_attribute_value pvav5
    ON pvav5.attribute_id = pvat5.attribute_id
    AND pvav5.variation_id = pvc.variation_id AND pvav5.company_id = pvc.company_id

WHERE pvc.company_id=@company """,
    ).to_pandas()

    # Merge together information to menu product with preferences
    df_menu_products_with_preferences = df_menu_with_preferences.merge(
        df_products,
        on="variation_id",
        how="left",
    )

    # Replace nan values for no preferences with empty bracked []
    df_menu_products_with_preferences = df_menu_products_with_preferences.astype(
        {"variation_price": float},
    )

    await cache.write_pandas(df_menu_products_with_preferences)

    return df_menu_products_with_preferences


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


class CompareWeekState(BaseModel):
    agreement_id: int
    year: int
    week: int


async def compare_week(state: CompareWeekState) -> None:
    import os

    version = os.getenv("TAG")
    if version:
        with st.expander("Version"):
            st.write(version)

    st.title(f"Compare for year: {state.year} - week: {state.week}")

    back, _, forward = st.columns([1, 5, 1])

    if back.button("<-  Go to previous week"):
        set_deeplink(
            CompareWeekState(
                agreement_id=state.agreement_id,
                year=state.year,
                week=state.week - 1,
            ),
        )
    if forward.button("Go to next week  ->"):
        set_deeplink(
            CompareWeekState(
                agreement_id=state.agreement_id,
                year=state.year,
                week=state.week + 1,
            ),
        )

    shown_recipe_ids = read_cache("shown_recipe_ids")

    if not shown_recipe_ids:
        shown_recipe_ids = dict[int, list[int]]()
        set_cache("shown_recipe_ids", shown_recipe_ids)

    with st.spinner("Loading Customer data..."):
        customers = await cache_awaitable(
            f"customer_{state.agreement_id}",
            customer_information([state.agreement_id]),
        )

        if customers.empty:
            st.error(f"Could not find customer with agreement id {state.agreement_id}")
            return

        customer = PreselectorCustomer(**customers.iloc[0].to_dict())

    year = state.year
    week = state.week

    st.write(
        f"Creating a menu with {customer.number_of_recipes} recipes and {customer.portion_size} portions",
    )

    def to_next_week() -> None:
        return
        set_deeplink(
            CompareWeekState(
                agreement_id=customer.agreement_id,
                year=year,
                week=week + 1,
            ),
        )

    def answered_year_weeks() -> list[tuple[int, int]]:
        reference_date = date(year, 1, 1)
        reference_date = reference_date + timedelta(weeks=week)

        current_date = date.today() + timedelta(weeks=1)
        current_year = current_date.isocalendar()[0]
        current_week = current_date.isocalendar()[1]

        year_weeks = []

        while current_date < reference_date:
            year_weeks.append((current_year, current_week))
            current_date = current_date + timedelta(weeks=1)
            current_year = current_date.isocalendar()[0]
            current_week = current_date.isocalendar()[1]

        return year_weeks

    def view_results() -> None:
        return
        year_weeks = answered_year_weeks()
        st.info("Showing results.")

        set_deeplink(
            ShowChoicesState(agreement_id=customer.agreement_id, year_weeks=year_weeks),
        )

    shown_recipe_ids_key = year * 100 + week
    if len(shown_recipe_ids.keys()) > 5 and shown_recipe_ids_key not in shown_recipe_ids:  # noqa: PLR2004
        view_results()

    if st.button("Skip weeks and view results"):
        view_results()

    with st.spinner("Loading needed data"):
        store = preselector_store()

        store = await load_cache(
            store,
            company_id=customer.company_id,
        )

    selected_recipe_ids = set()

    for shown_yearweek, main_recipe_ids in shown_recipe_ids.items():
        if shown_yearweek < year * 100 + week:
            selected_recipe_ids.update(main_recipe_ids)

    request = GenerateMealkitRequest(
        agreement_id=customer.agreement_id,
        company_id=customer.company_id,
        compute_for=[
            YearWeek(week=week, year=year)
        ],
        taste_preferences=[
            NegativePreference(
                preference_id=pref_id,
                is_allergy=False
            )
            for pref_id in customer.taste_preference_ids
        ] if customer.taste_preference_ids else [],
        concept_preference_ids=[customer.concept_preference_id],
        portion_size=customer.portion_size,
        number_of_recipes=customer.number_of_recipes,
        quarentine_main_recipe_ids=list(selected_recipe_ids),
        override_deviation=False,
        has_data_processing_consent=True
    )


    with st.spinner("Running preselector..."):
        request = convert_concepts_to_attributes(request.to_upper_case_ids())
        preselector_result = await run_preselector_for_request(request, store)

    if not preselector_result.success:
        st.error(
            f"An error occurred when running the pre-selector: {preselector_result.failures[0].error_message}",
        )
        to_next_week()
        return
    else:
        response = preselector_result.success_response()
        assert response is not None
        result = response.year_weeks[0]

    if len(result.main_recipe_ids) == 0:
        st.error("No recipes found for the preselector.")
        to_next_week()
        return

    menu = await store.feature_view(PreselectorYearWeekMenu).filter(
        (pl.col("company_id") == customer.company_id) & (
            pl.col("menu_year") == year
        ) & (
            pl.col("menu_week") == week
        )
    ).to_pandas()

    with st.spinner("Fetching chef's selection..."):
        cache_key = f"mealselector_{customer.model_dump_json()}_{year}_{week}"

        mealselector_result = read_cache(cache_key)

        def set_api_token(token: str) -> None:
            set_cache("mealselector_token", token)

        if not mealselector_result:
            mealselector_result = await run_mealselector(
                customer,
                year,
                week,
                menu,
                token=read_cache("mealselector_token"),
                on_new_token=set_api_token,
            )
            set_cache(cache_key, mealselector_result)

    if isinstance(mealselector_result, Exception) or not mealselector_result:
        st.error(
            f"An error occurred when fetching the chef-selection: {mealselector_result}",
        )
        to_next_week()
        return

    if len(mealselector_result) == 0:
        st.info("No recipes found for the mealselector")
        to_next_week()

    shown_recipe_ids[year * 100 + week] = list(
        result.main_recipe_ids,
    ) + list(mealselector_result)
    set_cache("shown_recipe_ids", shown_recipe_ids)

    with st.spinner("Loading recipe information..."):
        pre_selector_recipe_info = await cached_recipe_info(
            main_recipe_ids=result.main_recipe_ids,
            year=year,
            week=week,
        )
        # other_recipe_info = pre_selector_recipe_info
        other_recipe_info = await cached_recipe_info(
            main_recipe_ids=mealselector_result,
            year=year,
            week=week,
        )

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
    left.write(infos[0][0])

    right = right.container(border=True)
    right.header("Mealkit B")
    mealkit(infos[1][1], right)
    right.write(infos[1][0])

    left.markdown("---")
    right.markdown("---")

    # change_a = left.radio(
    #     "Optional - How many would you change in A?",
    #     options=list(range(len(infos[0][1]) + 1)),
    #     index=None,
    # )
    # change_b = right.radio(
    #     "Optional - How many would you change in B?",
    #     options=list(range(len(infos[0][1]) + 1)),
    #     index=None,
    # )

    # if change_a is None or change_b is None:

    # if change_a == change_b:
    # if left.button("I choose mealkit A"):
    #     set_deeplink(
    #         CompareWeekState(
    #             agreement_id=state.agreement_id,
    #             year=state.year,
    #             week=state.week + 1,
    #         ),
    #     )
        # set_deeplink(
        #     ExplainSelectionState(
        #         agreement_id=customer.agreement_id,
        #         year=year,
        #         week=week,
        #         selected_create=infos[0][0],
        #         has_order_history=False,
        #         selected_recipe_ids=infos[0][1]["main_recipe_id"].tolist(),
        #         other_recipe_ids=infos[1][1]["main_recipe_id"].tolist(),
        #         selected_number_of_changes=int(change_a) if change_a else None,
        #         other_number_of_changes=int(change_b) if change_b else None,
        #     ),
        # )

    # if right.button("I choose mealkit B"):
    #     set_deeplink(
    #         CompareWeekState(
    #             agreement_id=state.agreement_id,
    #             year=state.year,
    #             week=state.week + 1,
    #         ),
    #     )
        # set_deeplink(
        #     ExplainSelectionState(
        #         agreement_id=customer.agreement_id,
        #         year=year,
        #         week=week,
        #         has_order_history=False,
        #         selected_create=infos[1][0],
        #         selected_recipe_ids=infos[1][1]["main_recipe_id"].tolist(),
        #         other_recipe_ids=infos[0][1]["main_recipe_id"].tolist(),
        #         selected_number_of_changes=int(change_b) if change_b else None,
        #         other_number_of_changes=int(change_a) if change_a else None,
        #     ),
        # )
    #
    #     set_deeplink(
    #         ExplainSelectionState(
    #
