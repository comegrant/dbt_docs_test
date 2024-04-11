import logging
from typing import Annotated

import pandas as pd
import polars as pl
from aligned import Bool, Float, Int32, String, Timestamp, feature_view
from aligned.compiler.feature_factory import List
from data_contracts.recommendations.recipe import HistoricalRecipeOrders, RecipeCost, RecipeFeatures, RecipeNutrition
from data_contracts.sources import SqlServerConfig, adb, data_science_data_lake

logger = logging.getLogger(__name__)

preselector_ab_test_dir = data_science_data_lake.directory("preselector/ab-test")


async def historical_preselector_vector(agreement_id: int, year_weeks: list[tuple[int, int]]) -> pl.DataFrame:
    from preselector.new_main import BasketFeatures

    df = await HistoricalRecipeOrders.query().all().to_lazy_polars()

    year_week_number = [year * 100 + week for year, week in year_weeks]

    df = df.filter(
        pl.col("agreement_id") == agreement_id,
        (pl.col("year") * 100 + pl.col("week")).is_in(year_week_number),
    )

    nutrition = RecipeNutrition.query().features_for(df)
    cost = RecipeCost.query().features_for(df)

    vectors = (
        await RecipeFeatures.query()
        .features_for(df)
        .job.join(nutrition, method="inner", left_on="recipe_id", right_on="recipe_id")
        .with_request(nutrition.retrival_requests)  # Hack to get around a join bug
        .join(cost, method="inner", left_on=["recipe_id", "portion_size"], right_on=["recipe_id", "portion_size"])
        .rename({"agreement_id": "basket_id"})
        .aggregate(BasketFeatures.query().request)
        .to_polars()
    )

    return vectors.select(pl.exclude("basket_id"))


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
        lambda x: x["taxonomie_names"].str.split(", ").apply(lambda x: list(set(x))),
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
        rec.main_recipe_id IN ({', '.join([str(x) for x in main_recipe_ids])})
        AND rec.recipes_year = {year}
        AND rec.recipes_week = {week}) as recipes
WHERE recipes.nr = 1"""

    if not database:
        database = adb

    query = database.fetch(recipe_sql)

    return await RecipeInformation.query().using_source(query).all().to_pandas()


@feature_view(
    name="preselector_test_choice",
    source=preselector_ab_test_dir.delta_at("preselector_test_result.delta"),
)
class PreselectorTestChoice:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    main_recipe_ids = List(Int32())
    number_of_recipes_to_change = Int32().is_optional()

    compared_main_recipe_ids = List(Int32())
    compared_number_of_recipes_to_change = Int32().is_optional()

    chosen_mealkit = String().accepted_values(["pre-selector", "chef-selection"])

    was_lower_cooking_time = Bool()
    was_more_variety = Bool()
    was_more_interesting = Bool()
    was_more_family_friendly = Bool()
    was_better_recipes = Bool()
    was_better_proteins = Bool()
    was_better_sides = Bool()
    was_better_images = Bool()
    was_fewer_unwanted_ingredients = Bool()
    had_recipes_last_week = Bool()

    created_at = Timestamp()
    updated_at = Timestamp()

    total_cost_of_food = Float().is_optional()
    concept_revenue = Float().is_optional()

    description = String().is_optional()


@feature_view(
    name="customer_information",
    source=preselector_ab_test_dir.delta_at("customer_information.delta"),
)
class CustomerInformation:
    agreement_id = Int32().as_entity()

    company_id = String()

    concept_preference_id = String()
    taste_preference_ids = List(String())

    portion_size = Int32()
    number_of_recipes = Int32()

    subscribed_product_variation_id = String()


async def customer_information(agreement_ids: list[int]) -> pd.DataFrame:
    concept_preference_type_id = "009cf63e-6e84-446c-9ce4-afdbb6bb9687"
    taste_preference_type_id = "4c679266-7dc0-4a8e-b72d-e9bb8dadc7eb"
    mealbox_product_type_id = "2f163d69-8ac1-6e0c-8793-ff0000804eb3"

    if not agreement_ids:
        return pd.DataFrame()

    customer_sql = f"""
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
        CONCAT(CONCAT('["', STRING_AGG(convert(nvarchar(36), bap.preference_id), '", "')), '"]') as preference_ids,
        CONCAT(CONCAT('["', STRING_AGG(pref.name, '","')), '"]') as preferences
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
        AND ba.agreement_id IN ({', '.join([str(x) for x in agreement_ids])})
    """

    return await (
        CustomerInformation.query()
        .using_source(adb.fetch(customer_sql))
        .all()
        .polars_method(
            lambda df: df.with_columns(pl.col("taste_preference_ids").cast(pl.List(pl.Utf8()))),
        )
        .to_pandas()
    )
