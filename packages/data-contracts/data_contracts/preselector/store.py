from aligned import Bool, FeatureStore, Float, Int32, List, String, Timestamp, feature_view
from data_contracts.sources import adb, data_science_data_lake, materialized_data

preselector_ab_test_dir = data_science_data_lake.directory("preselector/ab-test")


@feature_view(
    name="preselector_test_choice",
    source=preselector_ab_test_dir.delta_at("preselector_test_result_v2.delta"),
)
class PreselectorTestChoice:
    agreement_id = Int32().as_entity()
    year = Int32().as_entity()
    week = Int32().as_entity()

    preselector_version = String().is_optional().fill_na("rulebased_v1")

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


default_mealbox_query = """SELECT
    wm.menu_week as menu_week,
    wm.menu_year as menu_year,
    c.number_of_recipes,
    p.product_id as mealbox_id,
    p.variation_company_id as company_id,
    p.variation_portions as portion_size,
    p.variation_name,
    p.variation_id,
    CONCAT('[', CONCAT(STRING_AGG(r.recipe_id, ','), ']')) as recipe_ids,
    CONCAT('[', CONCAT(STRING_AGG(COALESCE(r.main_recipe_id, r.recipe_id), ','), ']')) as main_recipe_ids
FROM (
    VALUES
        (2, '2F163D69-8AC1-6E0C-8793-FF0000804EB3'),
        (3, '2F163D69-8AC1-6E0C-8793-FF0000804EB3'),
        (4, '2F163D69-8AC1-6E0C-8793-FF0000804EB3'),
        (5, '2F163D69-8AC1-6E0C-8793-FF0000804EB3')
) AS c(number_of_recipes, product_type_id)
INNER JOIN mb.products p ON p.product_type_id = c.product_type_id
INNER JOIN pim.WEEKLY_MENUS wm ON wm.COMPANY_ID = p.variation_company_id
INNER JOIN pim.MENUS m ON m.WEEKLY_MENUS_ID = wm.WEEKLY_MENUS_ID
INNER JOIN pim.MENU_VARIATIONS mv ON mv.MENU_ID = m.MENU_ID
    AND mv.MENU_VARIATION_EXT_ID = p.variation_id AND mv.MENU_NUMBER_DAYS = c.number_of_recipes
INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
INNER JOIN pim.recipes r ON mr.RECIPE_ID = r.recipe_id
GROUP BY
    menu_week,
    menu_year,
    number_of_recipes,
    product_id,
    variation_company_id,
    variation_portions,
    p.variation_name,
    p.variation_id"""


@feature_view(
    name="default_mealbox_recipes",
    source=adb.fetch(default_mealbox_query),
    materialized_source=materialized_data.delta_at("default_mealbox_recipes"),
)
class DefaultMealboxRecipes:
    variation_id = String().as_entity()
    menu_week = Int32().as_entity()
    menu_year = Int32().as_entity()

    mealbox_id = String()
    number_of_recipes = Int32()
    portion_size = Int32()

    company_id = String()
    variation_name = String()

    recipe_ids = List(Int32())
    main_recipe_ids = List(Int32())


recipe_preferences_sql = """WITH distinct_recipe_preferences AS (
    SELECT DISTINCT
        rp.PORTION_ID,
        pt.PORTION_SIZE as portion_size,
        r.recipe_id,
        r.main_recipe_id,
        rmt.recipe_name,
        p.preference_id,
        p.name,
        menu_year,
        menu_week
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id
        AND m.product_type_id in (
            'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1',
            '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
        ) --Velg&Vrak dishes
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.MENU_ID = m.menu_id
        AND mr.MENU_RECIPE_ORDER <= mv.MENU_NUMBER_DAYS
    INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
    INNER JOIN cms.company c ON c.id = wm.company_id
    INNER JOIN cms.country cont ON cont.id = c.country_id
    INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id
        AND rmt.language_id = cont.default_language_id
    INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
    INNER JOIN pim.portions pt ON pt.PORTION_ID = rp.PORTION_ID
    INNER JOIN pim.chef_ingredient_sections cis ON cis.RECIPE_PORTION_ID = rp.recipe_portion_id
    INNER JOIN pim.chef_ingredients ci ON ci.CHEF_INGREDIENT_SECTION_ID = cis.CHEF_INGREDIENT_SECTION_ID
    INNER JOIN pim.order_ingredients oi ON oi.ORDER_INGREDIENT_ID = ci.ORDER_INGREDIENT_ID
    INNER JOIN pim.ingredients i ON i.ingredient_internal_reference = oi.INGREDIENT_INTERNAL_REFERENCE
    INNER JOIN pim.find_ingredient_categories_parents icc ON icc.ingredient_id = i.ingredient_id
    INNER JOIN pim.ingredient_category_preference icp ON icp.ingredient_category_id = icc.parent_category_id
    INNER JOIN cms.preference p ON p.preference_id = icp.preference_id
        AND p.preference_type_id = '4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB'
    INNER JOIN cms.preference_company pc ON pc.company_id = wm.company_id
        AND pc.preference_id = p.preference_id AND pc.is_active = 1
    WHERE m.RECIPE_STATE = 1
)

SELECT
    PORTION_ID as portion_id,
    portion_size,
    menu_year,
    menu_week,
    recipe_id,
    main_recipe_id,
    CONCAT('["', CONCAT(STRING_AGG(convert(nvarchar(36), preference_id), '", "'), '"]')) as preference_ids,
    CONCAT('["', CONCAT(STRING_AGG(name, '", "'), '"]')) as preferences
    FROM distinct_recipe_preferences
    GROUP BY PORTION_ID, portion_size, menu_year, menu_week, recipe_id, main_recipe_id
"""


@feature_view(
    name="recipe_preferences",
    source=adb.fetch(recipe_preferences_sql),
    materialized_source=materialized_data.delta_at("recipe_preferences"),
)
class RecipePreferences:
    recipe_id = Int32().as_entity()
    portion_id = String().as_entity()

    portion_size = Int32()

    menu_year = Int32()
    menu_week = Int32()

    main_recipe_id = Int32()

    preference_ids = List(String())
    preferences = List(String())


def preselector_contracts() -> FeatureStore:
    store = FeatureStore.experimental()

    views = [PreselectorTestChoice, DefaultMealboxRecipes, RecipePreferences]

    for view in views:
        store.add_compiled_view(view.compile())

    return store
