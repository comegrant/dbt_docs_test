from aligned import Bool, EventTimestamp, Int32, String, feature_view
from rec_engine.contacts import Contacts
from rec_engine.sources import adb, adb_ml, folder

flex_dish_id = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
mealbox_id = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"

recommendation_engine_origin_id = "9e016a92-9e5c-4b5b-ac5d-739cefd6f07b".upper()
user_origin_id = "25017d0e-f788-48d7-8dc4-62581d58b698".upper()

contacts = [Contacts.matsmoll().markdown(), Contacts.niladri().markdown()]

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
    COALESCE(bao.cutoff_date, bao.created_date) as delivered_at,
    r.recipe_id,
    rr.RATING as rating
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
WHERE DATEDIFF(MONTH, dbo.find_first_day_of_week(bao.year, bao.week), GETDATE()) <=6
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


@feature_view(
    name="recipe_taxonomies",
    description="The taxonomies associated with a recipe.",
    materialized_source=folder.parquet_at("recipe_taxonomies.parquet"),
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
    materialized_source=folder.parquet_at("historical_recipe_orders.parquet"),
    source=adb_ml.fetch(historical_orders_sql),
    contacts=contacts,
)
class HistoricalRecipeOrders:
    agreement_id = Int32().as_entity()
    recipe_id = Int32().as_entity()

    delivered_at = EventTimestamp()

    rating = (Int32().is_optional().lower_bound(0).upper_bound(5)).description(
        "Avoid 0 values, as this can lead to odd predictions.",
    )


@feature_view(
    name="recipe_ingredients",
    materialized_source=folder.parquet_at("recipe_ingredients.parquet"),
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


@feature_view(
    name="basket_deviation",
    source=adb.with_schema("cms").table("billing_agreement_basket_deviation"),
    contacts=contacts,
)
class BasketDeviation:
    billing_agreement_basket_id = String().as_entity()
    week = Int32().as_entity()
    year = Int32().as_entity()

    billing_agreement_basket_deviation_id = String()

    is_active = Bool()
    origin = String()

    was_user, was_meal_selector = origin.one_hot_encode(
        [user_origin_id, recommendation_engine_origin_id],
    )
