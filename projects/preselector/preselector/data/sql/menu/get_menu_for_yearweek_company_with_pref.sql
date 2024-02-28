declare @company uniqueidentifier = '{company_id}', @week int = '{week}', @year int = '{year}';

with recipe_preferences AS (
    SELECT
        recipe_id,
        portion_id,
        STRING_AGG(convert(nvarchar(36), preference_id), ', ') as preference_ids
        FROM pim.recipes_category_preferences_mapping
        GROUP BY recipe_id, portion_id
    )


SELECT
    wm.menu_week,
    wm.menu_year,
    mv.menu_variation_ext_id as variation_id,
    r.main_recipe_id as main_recipe_id,
    rcpm.recipe_id,
    rcpm.portion_id,
    rcpm.preference_ids,
    rp.recipe_portion_id
  FROM pim.weekly_menus wm
  JOIN pim.menus m on m.weekly_menus_id = wm.weekly_menus_id
  JOIN pim.menu_variations mv on mv.menu_id = m.menu_id
  JOIN pim.menu_recipes mr on mr.menu_id = m.menu_id
  JOIN pim.recipes r on r.recipe_id = mr.RECIPE_ID
  INNER JOIN pim.recipe_portions rp ON rp.RECIPE_ID = r.recipe_id AND rp.portion_id = mv.PORTION_ID
  JOIN recipe_preferences rcpm on rcpm.recipe_id = mr.recipe_id and rcpm.portion_id = mv.portion_id
  where m.RECIPE_STATE = 1
  AND wm.menu_week = @week AND wm.menu_year = @year AND wm.company_id = @company
