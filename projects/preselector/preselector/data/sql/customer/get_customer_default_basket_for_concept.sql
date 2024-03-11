declare @company_id uniqueidentifier = '{company_id}';
declare @week int = {week};
declare @year int = {year};

WITH default_dishes AS (
    SELECT
        wm.menu_year
        , wm.menu_week
        , wm.company_id
        , mv.menu_variation_ext_id as variation_id
        , mv.variation_name
        , mr.recipe_id
        , mr.menu_recipe_order
        , r.main_recipe_id
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.menu_id = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
    INNER JOIN pim.recipes r on r.recipe_id = mr.RECIPE_ID
    WHERE wm.menu_year = @year
        AND wm.menu_week = @week
        AND wm.COMPANY_ID = @company_id)

SELECT
    ba.agreement_id
    , dd.menu_year as year
    , dd.menu_week as week
    , dd.main_recipe_id
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
INNER JOIN cms.billing_agreement_basket_product babp ON babp.billing_agreement_basket_id = bab.id
INNER JOIN product_layer.product_variation pv ON pv.id = babp.subscribed_product_variation_id
INNER JOIN product_layer.product p ON p.id = pv.product_id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id AND pvc.company_id = ba.company_id
INNER JOIN cms.billing_agreement_preference bap ON bap.agreement_id = ba.agreement_id
INNER JOIN cms.preference pref ON pref.preference_id = bap.preference_id
    AND pref.preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- concept
INNER JOIN cms.preference_attribute_value pav ON pav.attribute_value = pv.product_id
    AND pav.attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' -- linked_mealbox_id
    AND pav.preference_id = pref.preference_id
INNER JOIN default_dishes dd ON dd.variation_id = babp.subscribed_product_variation_id
order by agreement_id
