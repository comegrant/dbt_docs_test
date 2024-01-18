declare @company uniqueidentifier = '{company_id}', @week int = '{week}', @year int = '{year}';

WITH distinct_recipe_preferences AS (
    SELECT DISTINCT rp.recipe_portion_id, rmt.recipe_name, p.preference_id, p.name
        FROM pim.weekly_menus wm
        INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.product_type_id in ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', '2F163D69-8AC1-6E0C-8793-FF0000804EB3')--Velg&Vrak dishes
        INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
        INNER JOIN pim.menu_recipes mr ON mr.MENU_ID = m.menu_id AND mr.MENU_RECIPE_ORDER <= mv.MENU_NUMBER_DAYS
        INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
        INNER JOIN cms.company c ON c.id = wm.company_id
        INNER JOIN cms.country cont ON cont.id = c.country_id
        INNER JOIN pim.recipe_metadata_translations rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id AND rmt.language_id = cont.default_language_id
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
        INNER JOIN cms.preference_company pc ON pc.company_id = wm.company_id AND pc.preference_id = p.preference_id AND pc.is_active = 1
        where m.RECIPE_STATE = 1 AND wm.menu_week = @week AND wm.menu_year = @year AND wm.company_id = @company
)
SELECT
    recipe_portion_id,
    STRING_AGG(convert(nvarchar(36), preference_id), ', ') as preference_ids,
    STRING_AGG(name, ', ') as preferences
    FROM distinct_recipe_preferences
    GROUP BY recipe_portion_id
