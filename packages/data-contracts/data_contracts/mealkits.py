from datetime import timedelta

from aligned import (
    EventTimestamp,
    Float,
    Int32,
    List,
    String,
    feature_view,
)

from data_contracts.sources import adb, materialized_data

one_sub_mealkit_features = """WITH cost_of_food_target (variation_id, cost_of_food) AS (
    SELECT pvat.variation_id, pvat.attribute_value
    FROM product_layer.product_variation_attribute_value pvat
    WHERE pvat.attribute_id = '202ce956-f742-4098-814b-5c3c90ee663e'
),

number_of_portions (variation_id, number_of_portions) AS (
    SELECT pvat.variation_id, pvat.attribute_value
    FROM product_layer.product_variation_attribute_value pvat
    WHERE pvat.attribute_id = 'D376B880-C630-434C-9EB3-E4009DBBCF8C'
),

number_of_recipes (variation_id, number_of_recipes) AS (
    SELECT pvat.variation_id, CAST(pvat.attribute_value AS INTEGER)
    FROM product_layer.product_variation_attribute_value pvat
    WHERE pvat.attribute_id = '4D4F9B2C-A876-4A1F-B17C-F4500F577202'
),

selling_price (variation_id, price) AS (
    SELECT pvat.variation_id, pvat.attribute_value
    FROM product_layer.product_variation_attribute_value pvat
    WHERE pvat.attribute_id = '3DDC86D3-519A-4CED-AB4C-A53CEB98FFC5'
)

SELECT
    pv.product_id,
    pvc.name,
    cof.cost_of_food,
    COALESCE(np.number_of_portions, 4) as number_of_portions,
    nr.number_of_recipes,
    sp.price,
    pvc.company_id,
    GETDATE() as loaded_at
FROM product_layer.product_variation_company pvc
INNER JOIN product_layer.product_variation pv on pv.id = pvc.variation_id
LEFT JOIN cost_of_food_target cof ON pvc.variation_id = cof.variation_id
LEFT JOIN number_of_portions np ON pvc.variation_id = np.variation_id
LEFT JOIN number_of_recipes nr ON pvc.variation_id = nr.variation_id
LEFT JOIN selling_price sp ON pvc.variation_id = sp.variation_id
WHERE pv.product_id = 'd699150e-d2de-4bc1-a75c-8b70c9b28ae3'"""

@feature_view(
    name="one_sub_mealkits",
    source=adb.fetch(one_sub_mealkit_features),
    materialized_source=materialized_data.parquet_at("one_sub_mealkits.parquet"),
    acceptable_freshness=timedelta(days=5),
)
class OneSubMealkits:
    company_id = String().as_entity()
    number_of_recipes = Int32().as_entity()
    number_of_portions = Int32().as_entity()

    name = String()
    product_id = String()

    loaded_at = EventTimestamp()

    cost_of_food = Float()
    price = Float().description("The selling price of the mealkit")

    cost_of_food_target_percentage = cost_of_food / price
    cost_of_food_target_per_recipe = cost_of_food / number_of_recipes


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
    source=adb.fetch(default_mealbox_query).with_loaded_at(),
    materialized_source=materialized_data.parquet_at("default_mealbox_recipes.parquet"),
)
class DefaultMealboxRecipes:
    variation_id = String().as_entity()
    menu_week = Int32().as_entity()
    menu_year = Int32().as_entity()

    number_of_recipes = Int32().as_entity()
    portion_size = Int32().as_entity()
    company_id = String().as_entity()

    loaded_at = EventTimestamp()
    mealbox_id = String()

    variation_name = String()

    recipe_ids = List(Int32())
    main_recipe_ids = List(Int32())
