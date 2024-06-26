-- the menus that are planned, to be used as the "master table for prediction"
DECLARE @COMPANY_ID UNIQUEIDENTIFIER = '{company_id}' GL;

WITH weekly_plan AS (
    SELECT
        menu_year AS year,
        menu_week AS week,
        weekly_menus_id,
        company_id
    FROM
        pim.weekly_menus
    WHERE company_id = @COMPANY_ID
),
menus AS (
    SELECT
        menu_id,
        weekly_menus_id,
        product_type_id
    FROM pim.menus
),

menu_variations AS (
    SELECT
        menu_id,
        menu_variation_ext_id AS variation_id,
        menu_number_days,
        portion_id
    FROM pim.menu_variations
)

SELECT  DISTINCT
    weekly_plan.year,
    weekly_plan.week,
    1 AS is_menu_week
FROM weekly_plan
LEFT JOIN
    menus
    ON weekly_plan.weekly_menus_id = menus.weekly_menus_id
LEFT JOIN
    menu_variations
    ON menu_variations.menu_id = menus.menu_id
