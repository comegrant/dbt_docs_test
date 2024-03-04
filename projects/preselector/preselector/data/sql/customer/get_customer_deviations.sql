
declare @company_id uniqueidentifier = '{company_id}';
declare @week int = {week};
declare @year int = {year};

DECLARE @PRODUCT_TYPE_ID_FLEX uniqueidentifier = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'; -- Flex / Velg&Vrak
DECLARE @PRODUCT_TYPE_ID_MEALBOX uniqueidentifier = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'; -- Mealbox

SELECT
    ba.agreement_id
    , babd.year
    , babd.week
    , r2.main_recipe_id
FROM cms.billing_agreement ba
INNER JOIN cms.company c on ba.company_id = c.id
INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
INNER JOIN cms.billing_agreement_basket_deviation babd ON babd.billing_agreement_basket_id = bab.id
INNER JOIN cms.billing_agreement_basket_deviation_product babdp ON babdp.billing_agreement_basket_deviation_id = babd.id
INNER JOIN product_layer.product_variation pv ON pv.id = babdp.subscribed_product_variation_id
INNER JOIN product_layer.product p ON p.id = pv.product_id AND p.product_type_id = @PRODUCT_TYPE_ID_FLEX
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id AND pvc.company_id = ba.company_id
INNER JOIN pim.weekly_menus wm ON wm.menu_week = babd.week AND wm.menu_year = babd.year AND wm.company_id = ba.company_id
INNER JOIN pim.MENUS m ON m.MENU_NAME = p.name AND wm.weekly_menus_id = m.WEEKLY_MENUS_ID
INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.MENU_ID
INNER JOIN pim.recipes r2 ON r2.recipe_id = mr.RECIPE_ID
    WHERE babd.week = @week
    AND babd.year = @year
    AND babd.is_active = 1
    AND babd.origin IN (
        '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B', -- mealselector (appears as rec engine/rec engine batch)
        '25017d0e-f788-48d7-8dc4-62581d58b698' -- other deviations, not mealselector (such as user)
    )
    AND ba.company_id = @company_id
    order by agreement_id
