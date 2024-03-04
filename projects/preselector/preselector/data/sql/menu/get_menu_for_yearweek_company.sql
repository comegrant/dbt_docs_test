declare @company uniqueidentifier = '{company_id}', @week int = '{week}', @year int = '{year}';

SELECT
    wm.menu_year as year
    ,	wm.menu_week as week
    ,	c.company_name
    ,	m.menu_name as product
    ,   m.product_type_id
    ,	mv.variation_name as variation
    ,   mv.MENU_VARIATION_EXT_ID as variation_id
    ,   mr.RECIPE_ID as menu_recipe
    ,   r.main_recipe_id as recipe_id
    ,   rp.recipe_portion_id
    FROM pim.weekly_menus wm
        INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id
        AND m.product_type_id in ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', '2F163D69-8AC1-6E0C-8793-FF0000804EB3')
        INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
        INNER JOIN pim.menu_recipes mr ON mr.MENU_ID = m.menu_id
        INNER JOIN pim.recipes r ON r.recipe_id = mr.RECIPE_ID
        INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
        INNER JOIN cms.company c ON c.id = wm.company_id
        WHERE m.RECIPE_STATE = 1
            AND wm.menu_year = @year
            AND wm.menu_week = @week
            AND c.id = @company
        order by wm.menu_year, wm.menu_week, mv.MENU_VARIATION_EXT_ID
