import polars as pl
from aligned import Bool, CustomMethodDataSource, EventTimestamp, Float, Int32, List, String, Timestamp, feature_view
from data_contracts.sources import adb, adb_ml, materialized_data
from project_owners.owner import Owner

flex_dish_id = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
mealbox_id = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"

recommendation_engine_origin_id = "9e016a92-9e5c-4b5b-ac5d-739cefd6f07b".upper()
user_origin_id = "25017d0e-f788-48d7-8dc4-62581d58b698".upper()

contacts = [Owner.matsmoll().markdown(), Owner.niladri().markdown()]

taxonomies_sql = """
SELECT recipe_id, recipe_taxonomies, GETDATE() as loaded_at
FROM (SELECT r.recipe_id,
           tt.language_id,
           STRING_AGG(lower(trim(tt.TAXONOMIES_NAME)), ',') as recipe_taxonomies,
           MAX(COALESCE(t.modified_date, t.created_date))   as updated_at
    FROM pim.recipes_taxonomies rt
             INNER JOIN pim.TAXONOMIES t ON t.taxonomies_id = rt.taxonomies_id
             INNER JOIN pim.taxonomies_translations tt ON tt.taxonomies_id = rt.taxonomies_id
             INNER JOIN pim.recipes r ON r.recipe_id = rt.RECIPE_ID
    WHERE t.status_code_id = 1           -- Active
      AND t.taxonomy_type IN (1, 11, 12) -- Recipe
      AND r.main_recipe_id IS NOT NULL
    GROUP BY r.recipe_id, tt.language_id
) tax
"""

historical_orders_sql = """
WITH velgandvrak AS (
    SELECT * FROM mb.products p
    WHERE p.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' AND variation_portions != 1
)

SELECT
    ba.agreement_id,
    ba.company_id,
    COALESCE(bao.cutoff_date, bao.created_date) as delivered_at,
    p.variation_portions as portion_size,
    r.recipe_id,
    rr.RATING as rating,
    wm.MENU_WEEK as week,
    wm.MENU_YEAR as year
FROM cms.billing_agreement ba
         INNER JOIN cms.billing_agreement_order bao ON bao.agreement_id = ba.agreement_id
         INNER JOIN cms.billing_agreement_order_line baol ON baol.agreement_order_id = bao.id
         INNER JOIN velgandvrak p ON p.variation_id = baol.variation_id
         INNER JOIN pim.WEEKLY_MENUS wm
            ON wm.COMPANY_ID = ba.company_id
                AND wm.MENU_WEEK = bao.week
                AND wm.MENU_YEAR = bao.year
         INNER JOIN pim.MENUS m ON m.WEEKLY_MENUS_ID = wm.WEEKLY_MENUS_ID
         INNER JOIN pim.MENU_VARIATIONS mv ON mv.MENU_ID = m.MENU_ID AND mv.MENU_VARIATION_EXT_ID = baol.variation_id
         INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
         INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
         LEFT JOIN pim.RECIPES_RATING rr ON rr.RECIPE_ID = r.recipe_id AND rr.AGREEMENT_ID = ba.agreement_id
WHERE DATEDIFF(MONTH, dbo.find_first_day_of_week(bao.year, bao.week), GETDATE()) <=4
-- limiting to past 6 months of delivery date;
"""

recipe_ingredients_sql = """
SELECT
    recipe_id,
    CONCAT('["', STRING_AGG(REPLACE(ingredient_name, '"', ''), '","'), '"]') as all_ingredients,
    MAX(created_at) as loaded_at
FROM (
    SELECT rp.RECIPE_ID as recipe_id, it.INGREDIENT_NAME as ingredient_name, i.created_date as created_at
    FROM pim.RECIPE_PORTIONS rp
    INNER JOIN pim.PORTIONS p on p.PORTION_ID = rp.PORTION_ID
    INNER JOIN pim.CHEF_INGREDIENT_SECTIONS cis ON cis.recipe_portion_id = rp.recipe_portion_id
    INNER JOIN pim.chef_ingredients ci ON ci.chef_ingredient_section_id = cis.chef_ingredient_section_id
    INNER JOIN pim.order_ingredients oi ON oi.order_ingredient_id = ci.order_ingredient_id
    INNER JOIN pim.ingredients i on i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
    INNER JOIN pim.INGREDIENTS_TRANSLATIONS it ON it.INGREDIENT_ID = i.ingredient_id
    INNER JOIN pim.suppliers s ON s.supplier_id = i.supplier_id
    WHERE rp.CREATED_DATE > '2023-01-01' AND s.supplier_name != 'Basis'
) as ingredients
GROUP BY recipe_id
"""

recipe_features_sql = """WITH taxonomies AS (
    SELECT
        rt.RECIPE_ID as recipe_id,
        CONCAT('["', STRING_AGG(tt.TAXONOMIES_NAME, '", "'), '"]') as taxonomies
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
             rm.RECIPE_MAIN_INGREDIENT_ID as main_ingredient_id,
             rm.RECIPE_PHOTO as recipe_photo,
             rm.COOKING_TIME_FROM as cooking_time_from,
             rm.COOKING_TIME_TO as cooking_time_to,
             rmt.recipe_name,
             tx.taxonomies,
             ROW_NUMBER() over (PARTITION BY rec.recipe_id ORDER BY rmt.language_id) as nr
      FROM pim.recipes rec
        INNER JOIN pim.recipes_metadata rm ON rec.recipe_metadata_id = rm.RECIPE_METADATA_ID
        INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = rec.recipe_metadata_id
        INNER JOIN taxonomies tx ON tx.recipe_id = rec.recipe_id
) as recipes
WHERE recipes.nr = 1"""

deviation_sql = """SELECT
    r.recipe_id,
    bab.agreement_id,
    babd.is_active,
    babd.week,
    babd.year,
    babd.origin,
    babdp.updated_by,
    babdp.created_by,
    babdp.updated_at
FROM cms.billing_agreement_basket_deviation AS babd
INNER JOIN cms.billing_agreement_basket_deviation_product AS babdp
    ON babd.id = babdp.billing_agreement_basket_deviation_id
INNER JOIN cms.billing_agreement_basket AS bab ON babd.billing_agreement_basket_id = bab.id
INNER JOIN cms.billing_agreement AS ba ON bab.agreement_id = ba.agreement_id
INNER JOIN pim.WEEKLY_MENUS wm
    ON wm.COMPANY_ID = ba.company_id
	AND wm.MENU_WEEK = babd.week
	AND wm.MENU_YEAR = babd.year
INNER JOIN pim.MENUS m ON m.WEEKLY_MENUS_ID = wm.WEEKLY_MENUS_ID
INNER JOIN pim.MENU_VARIATIONS mv
    ON mv.MENU_ID = m.MENU_ID AND mv.MENU_VARIATION_EXT_ID = babdp.subscribed_product_variation_id
INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
WHERE babd.year >= 2024"""

user_subscription = """SELECT
    ba.agreement_id,
    ba.company_id,
    ba.status,
    ba.start_date,
    ba.method_code,
    ba.created_at,
    ba.updated_at,
    bab.delivery_week_type,
    bab.is_active,
    bab.timeblock,
    babp.subscribed_product_variation_id
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON ba.agreement_id = bab.agreement_id
INNER JOIN cms.billing_agreement_basket_product babp ON bab.id = babp.billing_agreement_basket_id"""


main_recipe_ids = {
    "fish": 1,
    "vegetarian": 3,
    "poultry": 5,
    "pork": 7,
    "beef": 8,
    "vegan": 12,
    "meat": 2,
    "shellfish": 4,
}


@feature_view(
    name="recipe_features",
    source=adb.fetch(recipe_features_sql),
    materialized_source=materialized_data.delta_at("recipe_features"),
)
class RecipeFeatures:
    recipe_id = Int32().as_entity()

    main_ingredient_id = Int32()

    year = Int32()
    week = Int32()

    recipe_name = String()

    cooking_time_from = Int32()
    cooking_time_to = Int32()

    taxonomies = List(String())

    is_family_friendly = taxonomies.contains("Familiefavoritter")
    is_kids_friendly = taxonomies.contains("Barnevennlig")

    (
        is_fish,
        is_vegetarian,
        is_poultry,
        is_pork,
        is_beef,
        is_vegan,
        is_meat,
        is_shellfish,
    ) = main_ingredient_id.one_hot_encode(
        [
            main_recipe_ids["fish"],
            main_recipe_ids["vegetarian"],
            main_recipe_ids["poultry"],
            main_recipe_ids["pork"],
            main_recipe_ids["beef"],
            main_recipe_ids["vegan"],
            main_recipe_ids["meat"],
            main_recipe_ids["shellfish"],
        ]
    )  # type: ignore


@feature_view(
    name="recipe_taxonomies",
    description="The taxonomies associated with a recipe.",
    materialized_source=materialized_data.delta_at("recipe_taxonomies"),
    source=adb_ml.fetch(taxonomies_sql),
    contacts=contacts,
)
class RecipeTaxonomies:
    recipe_id = Int32().as_entity()

    loaded_at = EventTimestamp()

    recipe_taxonomies = String().description(
        "All the taxonomies seperated by a ',' char.",
    )


@feature_view(
    name="historical_recipe_orders",
    description="The recipes that our customers have recived. Together with the rating of the dish.",
    source=adb_ml.fetch(historical_orders_sql),
    materialized_source=materialized_data.delta_at("historical_recipe_orders"),
    contacts=contacts,
)
class HistoricalRecipeOrders:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    company_id = String()
    week = Int32()
    year = Int32()

    portion_size = Int32()

    delivered_at = EventTimestamp()

    rating = (Int32().is_optional().lower_bound(0).upper_bound(5)).description(
        "A value of 0 means that the user did not make the dish. " "While a missing values means did not rate",
    )
    did_make_dish = rating != 0


@feature_view(
    name="user_subscription",
    source=adb.fetch(user_subscription),
    materialized_source=materialized_data.delta_at("user_subscription"),
)
class UserSubscription:
    agreement_id = Int32().as_entity()

    company_id = String()
    status = Int32()
    start_date = Timestamp()

    method_code = String()

    created_at = Timestamp()
    updated_at = Timestamp()

    delivery_week_type = Int32()
    is_active = Bool()
    timeblock = Int32()

    subscribed_product_variation_id = String()


@feature_view(
    name="raw_deviated_recipes",
    source=adb.fetch(deviation_sql),
    materialized_source=materialized_data.delta_at("deselected_recipes"),
)
class DeselectedRecipes:
    recipe_id = Int32().as_entity()
    agreement_id = Int32().as_entity()

    is_active = Bool()

    week = Int32()
    year = Int32()

    origin = String()

    created_by = String()
    updated_by = String()
    updated_at = EventTimestamp()


async def transform_deviations(req, limit) -> pl.LazyFrame:  # noqa: ANN001, ARG001
    from data_contracts.preselector.store import DefaultMealboxRecipes

    subs = (
        await UserSubscription.query()
        .select_columns(["agreement_id", "subscribed_product_variation_id", "company_id"])
        .to_polars()
    )

    unique_users = subs["agreement_id"].unique()
    all_orders = await HistoricalRecipeOrders.query().all().to_lazy_polars()

    grouped_orders = all_orders.groupby(["agreement_id", "week", "year"]).agg(
        pl.col("recipe_id").alias("ordered_recipe_ids"),
    )

    default_boxes = (
        all_orders.filter(pl.col("agreement_id").is_in(unique_users))
        .select([pl.col("agreement_id"), pl.col("week"), pl.col("year")])
        .join(subs.select(["agreement_id", "subscribed_product_variation_id", "company_id"]).lazy(), on="agreement_id")
        .rename({"subscribed_product_variation_id": "variation_id", "week": "menu_week", "year": "menu_year"})
    )

    default_baskets = (
        await DefaultMealboxRecipes.query()
        .select({"recipe_ids", "variation_name"})
        .all()
        .drop_invalid()
        .to_lazy_polars()
    ).with_columns(
        pl.col("menu_week").cast(pl.Int32),
        pl.col("menu_year").cast(pl.Int32),
    )

    grouped_orders = grouped_orders.join(
        default_boxes, left_on=["agreement_id", "week", "year"], right_on=["agreement_id", "menu_week", "menu_year"]
    ).collect()

    df = (
        grouped_orders.lazy()
        .join(
            default_baskets,
            left_on=["variation_id", "week", "year"],
            right_on=["variation_id", "menu_week", "menu_year"],
        )
        .collect()
    )

    df = (
        df.lazy()
        .with_columns(
            pl.col("ordered_recipe_ids").list.set_difference(pl.col("recipe_ids")).alias("selected_recipe_ids"),
            pl.col("recipe_ids").list.set_difference(pl.col("ordered_recipe_ids")).alias("deselected_recipe_ids"),
            pl.col("recipe_ids").alias("starting_recipe_ids"),
        )
        .unique(["agreement_id", "week", "year"])
    )

    df = df.select(
        [
            "company_id",
            "agreement_id",
            "week",
            "year",
            "selected_recipe_ids",
            "deselected_recipe_ids",
            "starting_recipe_ids",
            "ordered_recipe_ids",
        ]
    ).collect()

    return df.lazy()


@feature_view(
    name="mealbox_changes",
    source=CustomMethodDataSource.from_methods(
        all_data=transform_deviations,
    ),
    materialized_source=materialized_data.delta_at("mealbox_changes"),
)
class MealboxChanges:
    agreement_id = Int32().as_entity()
    week = Int32().as_entity()
    year = Int32().as_entity()

    company_id = String()

    selected_recipe_ids = List(Int32())
    deselected_recipe_ids = List(Int32())
    starting_recipe_ids = List(Int32())
    ordered_recipe_ids = List(Int32())


def mealbox_changes_as_rating(df: pl.LazyFrame) -> pl.LazyFrame:
    dislikes = (
        df.select(["deselected_recipe_ids", "agreement_id", "year", "week", "company_id"])
        .explode("deselected_recipe_ids")
        .with_columns(pl.col("deselected_recipe_ids").alias("recipe_id"), pl.lit(0).alias("rating"))
        .filter(pl.col("recipe_id").is_not_null())
        .collect()
    )

    likes = (
        df.select(["selected_recipe_ids", "agreement_id", "year", "week", "company_id"])
        .explode("selected_recipe_ids")
        .with_columns(pl.col("selected_recipe_ids").alias("recipe_id"), pl.lit(4).alias("rating"))
        .filter(pl.col("recipe_id").is_not_null())
        .collect()
    )

    columns = ["company_id", "recipe_id", "agreement_id", "year", "week", "rating"]

    new = dislikes.select(columns).vstack(likes.select(columns))
    return new.lazy()


@feature_view(
    name="mealbox_changes_as_rating",
    source=MealboxChanges.as_source().transform_with_polars(mealbox_changes_as_rating),
    materialized_source=materialized_data.parquet_at("mealbox_changes_as_rating.parquet"),
)
class MealboxChangesAsRating:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    week = Int32()
    year = Int32()

    company_id = String()

    rating = Int32().lower_bound(0).upper_bound(5)


@feature_view(
    name="recipe_ingredients",
    materialized_source=materialized_data.delta_at("recipe_ingredients"),
    source=adb_ml.fetch(recipe_ingredients_sql),
    description="All non base ingredients that a recipe contains.",
    contacts=contacts,
)
class RecipeIngredient:
    recipe_id = Int32().as_entity()

    loaded_at = EventTimestamp()

    all_ingredients = String().description(
        "All the ingredients seperated by a ',' char.",
    )

    contains_salmon = all_ingredients.contains("laks")
    contains_chicken = all_ingredients.contains("kylling")
    contains_meat = all_ingredients.contains("kjøtt")


@feature_view(
    name="basket_deviation",
    source=adb.with_schema("cms").table("billing_agreement_basket_deviation"),
    materialized_source=materialized_data.delta_at("basket_deviation"),
    contacts=contacts,
)
class BasketDeviation:
    billing_agreement_basket_id = String().as_entity()
    week = Int32().as_entity()
    year = Int32().as_entity()

    created_by = String()
    updated_by = String()

    is_active = Bool()
    origin = String()

    was_user, was_meal_selector = origin.one_hot_encode(
        [user_origin_id, recommendation_engine_origin_id],
    )


@feature_view(
    name="recipe_nutrition",
    source=adb.with_schema("pim").table("RECIPE_NUTRITION_FACTS"),
    materialized_source=materialized_data.parquet_at("recipe_nutrition.parquet"),
)
class RecipeNutrition:
    recipe_id = Int32().as_entity()
    portion_size = Int32().as_entity()

    energy_kcal_100g = Float().lower_bound(0).is_optional()
    carbs_100g = Float().lower_bound(0).is_optional()
    fat_100g = Float().lower_bound(0).is_optional()
    fat_saturated_100g = Float().lower_bound(0).is_optional()
    protein_100g = Float().lower_bound(0).is_optional()
    fruit_veg_fresh_100g = Float().lower_bound(0).is_optional()


@feature_view(
    name="recipe_cost",
    source=adb.with_schema("mb").table(
        "recipe_costs_pim",
        mapping_keys={
            "PORTION_SIZE": "portion_size",
            "PORTION_ID": "portion_id",
            "recipe_cost_whole_units_pim": "recipe_cost_whole_units",
            "recipes_year": "menu_year",
            "recipes_week": "menu_week",
        },
    ),
    materialized_source=materialized_data.delta_at("recipe_cost"),
)
class RecipeCost:
    recipe_id = Int32().as_entity()
    portion_size = Int32().as_entity()

    menu_year = Int32()
    menu_week = Int32()

    country = String()
    company_name = String()

    main_recipe_id = Int32()
    recipe_name = String()
    portions = String().description(
        "Needs to be a string because we have instances of '2+' in Danmark.",
    )
    portion_id = Int32()

    recipe_cost_whole_units = Float()

    price_category_max_price = Int32()
    price_category_level = Int32()

    is_premium = price_category_level >= 4  # noqa: PLR2004
    is_cheep = price_category_level <= -1

    suggested_selling_price_incl_vat = Float()


year_week_menu_sql = """SELECT DISTINCT (menu_year * 100 + menu_week) as yearweek,
    wm.menu_year as year,
    wm.menu_week as week,
    mr.recipe_id,
    m.MENU_EXTERNAL_ID as product_id,
    company_id
FROM pim.MENU_RECIPES mr
INNER JOIN pim.MENUS m ON m.MENU_ID = mr.MENU_ID AND m.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
INNER JOIN pim.weekly_menus wm ON m.WEEKLY_MENUS_ID = wm.weekly_menus_id
INNER JOIN pim.MENU_VARIATIONS mv ON m.MENU_ID = mv.MENU_ID
WHERE mv.PORTION_ID != 7 -- Removing protion size of 1"""


@feature_view(
    name="year_week_menu",
    description="Functions more like a fact table, as it mostly contains IDs to other info.",
    source=adb.fetch(year_week_menu_sql),
    materialized_source=materialized_data.delta_at("year_week_menu"),
)
class YearWeekMenu:
    yearweek = Int32().as_entity()
    year = Int32()
    week = Int32()
    recipe_id = Int32()
    product_id = String()
    company_id = String()
