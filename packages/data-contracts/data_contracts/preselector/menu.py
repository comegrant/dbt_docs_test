from datetime import timedelta

import polars as pl
from aligned import (
    CustomMethodDataSource,
    EventTimestamp,
    Float,
    Int32,
    String,
    feature_view,
)
from data_contracts.sources import adb, azure_dl_creds, materialized_data

preselector_menu_sql = """WITH menu (
  menu_week,
  menu_year,
  variation_id,
  main_recipe_id,
  recipe_id,
  portion_id,
  recipe_portion_id,
  menu_recipe_order,
  company_id
) AS (
  SELECT
    wm.menu_week,
    wm.menu_year,
    mv.menu_variation_ext_id as variation_id,
    r.main_recipe_id as main_recipe_id,
    r.recipe_id,
    mv.portion_id,
    rp.recipe_portion_id,
    mr.menu_recipe_order,
    wm.company_id
  FROM pim.weekly_menus wm
  JOIN pim.menus m on m.weekly_menus_id = wm.weekly_menus_id
  JOIN pim.menu_variations mv on mv.menu_id = m.menu_id
  JOIN pim.menu_recipes mr on mr.menu_id = m.menu_id
  JOIN pim.recipes r on r.recipe_id = mr.RECIPE_ID
  INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
  where m.RECIPE_STATE = 1
  AND (wm.menu_year * 100 + wm.menu_week) > 202340
)

SELECT
    p.id AS product_id ,
    p.name AS product_name ,
    pt.product_type_name AS product_type ,
    pt.product_type_id ,
    pv.id AS variation_id ,
    pv.sku AS variation_sku ,
    pvc.name AS variation_name ,
    pvc.company_id AS company_id ,
    ISNULL(pvav.attribute_value, pvat.default_value) AS variation_meals ,
    ISNULL(pvav2.attribute_value, pvat2.default_value) AS variation_portions ,
    ISNULL(pvav4.attribute_value, pvat4.default_value) AS variation_price ,
    ROUND(
        CAST(ISNULL(pvav4.attribute_value, pvat4.default_value) AS FLOAT
    ) * (
        1.0 + (CAST(ISNULL(pvav5.attribute_value, pvat5.default_value) AS FLOAT) / 100)), 2
    ) AS variation_price_incl_vat ,
    ISNULL(pvav5.attribute_value, pvat5.default_value) AS variation_vat,
    m.main_recipe_id,
    m.menu_week,
    m.menu_year,
    m.portion_id,
    m.recipe_id,
    m.menu_recipe_order
FROM product_layer.product p
INNER JOIN product_layer.product_type pt ON pt.product_type_id = p.product_type_id
INNER JOIN product_layer.product_variation pv ON pv.product_id = p.id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id
INNER JOIN menu m ON m.variation_id = pv.id AND m.company_id = pvc.company_id
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
WHERE pt.product_type_id = 'cac333ea-ec15-4eea-9d8d-2b9ef60ec0c1'
"""


@feature_view(
    name="preselector_year_week_menu",
    description="The different year week menus, but with portion sizes.",
    source=adb.fetch(preselector_menu_sql).with_loaded_at(),
    materialized_source=materialized_data.partitioned_parquet_at(
        "preselector_year_week_menu", partition_keys=["company_id", "menu_year"]
    ),
    acceptable_freshness=timedelta(days=6),
)
class PreselectorYearWeekMenu:
    recipe_id = Int32().as_entity()
    portion_id = Int32().as_entity()

    loaded_at = EventTimestamp()

    menu_week = Int32()
    menu_year = Int32()
    menu_recipe_order = Int32().is_optional()

    main_recipe_id = Int32()
    variation_id = String()
    product_id = String()

    variation_portions = Int32()
    company_id = String()


async def transform_recipe_cost(req, limit) -> pl.LazyFrame:  # noqa: ANN001
    from data_contracts.recipe import RecipeCost

    frame = await RecipeCost.query().all().to_lazy_polars()
    company_name_mapping = pl.DataFrame(
        {
            "company_name": ["Linas Matkasse", "Godtlevert", "Adams Matkasse", "RetNemt"],
            "company_id": [
                "6A2D0B60-84D6-4830-9945-58D518D27AC2",
                "09ECD4F0-AE58-4539-8E8F-9275B1859A19",
                "8A613C15-35E4-471F-91CC-972F933331D7",
                "5E65A955-7B1A-446C-B24F-CFE576BF52D7",
            ],
        }
    )
    with_id = frame.join(company_name_mapping.lazy(), on="company_name")

    return with_id.group_by(["menu_year", "menu_week", "portion_size", "company_id"]).agg(
        min_cost_of_food=pl.col("recipe_cost_whole_units").min(),
        max_cost_of_food=pl.col("recipe_cost_whole_units").max(),
        mean_cost_of_food=pl.col("recipe_cost_whole_units").mean(),
        median_cost_of_food=pl.col("recipe_cost_whole_units").median(),
    )


@feature_view(
    name="menu_week_recipe_stats",
    source=CustomMethodDataSource.from_methods(all_data=transform_recipe_cost).with_loaded_at(),
    materialized_source=materialized_data.parquet_at("meny_week_recipe_stats.parquet"),
    acceptable_freshness=timedelta(days=6),
)
class MenuWeekRecipeNormalization:
    menu_year = Int32().as_entity()
    menu_week = Int32().as_entity()
    portion_size = Int32().as_entity()
    company_id = String().as_entity()

    loaded_at = EventTimestamp()

    min_cost_of_food = Float()
    max_cost_of_food = Float()
    mean_cost_of_food = Float()
    median_cost_of_food = Float()


@feature_view(
    source=azure_dl_creds.directory("data-science/pre-selector").parquet_at("cost_of_food_per_menu_week.parquet"),
)
class CostOfFoodPerMenuWeek:
    year = Int32().as_entity()
    week = Int32().as_entity()
    company_id = String().as_entity()

    number_of_portions = Int32().as_entity()
    number_of_recipes = Int32().as_entity()

    target_cost_of_food = Float()

    cost_of_food_target_per_recipe = target_cost_of_food / number_of_recipes
