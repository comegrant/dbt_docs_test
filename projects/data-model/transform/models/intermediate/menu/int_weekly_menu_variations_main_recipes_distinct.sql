with 

menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, recipes as (

    select * from {{ ref('pim__recipes') }}

)

, menus_main_recipe_distinct as (

    select
        coalesce(recipes.main_recipe_id, menus.recipe_id) as main_recipe_id
        , count(distinct menus.menu_week_monday_date) as menu_week_count_main_recipe
        , max((menus.menu_year*100) + menus.menu_week) as previous_menu_week_main_recipe
        , datediff(week, min(menus.menu_week_monday_date), current_date()) as weeks_since_first_menu_week_main_recipe
    from menus
    left join recipes
        on menus.recipe_id = recipes.recipe_id
    where menus.recipe_id is not null
    group by 1

)

select * from menus_main_recipe_distinct