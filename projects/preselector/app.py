import asyncio
import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd
import streamlit as st
from data_contracts.sources import SqlServerConfig, adb, data_science_data_lake
from preselector.contracts.compare_boxes import RecipeInformation
from preselector.data.models.customer import PreselectorCustomer
from streamlit_pages import deeplinks, set_deeplink
from ui.deeplinks.collect_feedback import ExplainSelectionState, collect_feedback
from ui.deeplinks.compare_week import CompareWeekState, compare_week
from ui.deeplinks.show_results import ShowChoicesState, show_choices

logger = logging.getLogger(__name__)


preselector_ab_test_folder = data_science_data_lake.directory("preselector/ab-test")


def read_cache(key: str) -> Any | None:  # noqa: ANN401
    return st.session_state.get(key, None)


def set_cache(key: str, value: Any) -> None:  # noqa: ANN401
    st.session_state[key] = value


async def load_recipe_information(
    main_recipe_id: list[int],
    year: int,
    week: int,
    db_config: SqlServerConfig,
) -> pd.DataFrame:

    key = f"recipe_info_{year}_{week}"
    cache = read_cache(key) or dict()

    if cache is not None:
        assert isinstance(cache, dict)

    missing_ids = []
    infos = []

    for recipe_id in main_recipe_id:
        value = cache.get(recipe_id)
        if not value:
            missing_ids.append(recipe_id)
        else:
            infos.append(value)

    if not missing_ids:
        job = RecipeInformation.process_input(infos)
        return await job.to_pandas()

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
      WHERE
        rec.main_recipe_id IN ({', '.join([str(x) for x in missing_ids])})
        AND rec.recipes_year = {year}
        AND rec.recipes_week= {week}) as recipes
WHERE recipes.nr = 1"""

    data = await db_config.fetch(recipe_sql).to_polars()

    for value in data.to_dicts():
        infos.append(value)
        cache[value["recipe_id"]] = value

    set_cache(key, cache)
    job = RecipeInformation.process_input(infos)
    return await job.to_pandas()


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


async def load_agreement_id(email: str, db_config: SqlServerConfig) -> list[int] | Exception:
    key_value_cache_key = f"load_agreement_id{email}"
    cache_value = read_cache(key_value_cache_key)
    if cache_value is not None:
        return cache_value

    if '"' in email or "'" in email:
        return ValueError("Email must not be wrapped in quotes")

    agreement_sql = f"SELECT agreement_id FROM mb.customer_personalia WHERE email = '{email}'"

    df = await db_config.fetch(agreement_sql).to_pandas()
    agreement_ids = df["agreement_id"].tolist()

    set_cache(key_value_cache_key, agreement_ids)
    return agreement_ids


@deeplinks(
    {  # type: ignore
        CompareWeekState: compare_week,
        ExplainSelectionState: collect_feedback,
        ShowChoicesState: show_choices,
    },
)
async def test_preselector() -> None:
    import os

    st.set_page_config(page_title="Preselector A/B Test", page_icon="🍽️", layout="wide")

    version = os.getenv("TAG")
    if version:
        with st.expander("Version"):
            st.write(version)

    st.title("Preselector A/B Test")

    with st.form("Preselector"):
        email = st.text_input("Email")

        st.form_submit_button(label="Start comparison")

    if not email:
        return

    agreement_ids = await load_agreement_id(email, adb)

    if isinstance(agreement_ids, Exception):
        st.error(agreement_ids)
        return

    if len(agreement_ids) == 0:
        st.error("No agreement found for this email")
        return

    if len(agreement_ids) > 1:
        st.info("Multiple agreements found for this email")

        with st.form("Select agreement"):
            agreement_id = st.selectbox("Select agreement", agreement_ids)

            sub = st.form_submit_button(label="Start comparison")

        if not sub or agreement_id is None:
            return

        agreement_id = int(agreement_id)
    else:
        agreement_id = agreement_ids[0]

    current_date = date.today() + timedelta(weeks=1)
    year = current_date.isocalendar()[0]
    week = current_date.isocalendar()[1]

    set_deeplink(CompareWeekState(agreement_id=int(agreement_id), year=year, week=week))


if __name__ == "__main__":

    from combinations_app import main
    asyncio.run(main())
