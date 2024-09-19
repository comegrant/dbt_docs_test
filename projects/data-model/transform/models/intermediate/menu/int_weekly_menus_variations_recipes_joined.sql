with 

weekly_menus as (

    select * from {{ ref('pim__weekly_menus') }}

)

, menus as (

    select * from {{ ref('pim__menus') }}

)

, menu_variations as (

    select * from {{ ref('pim__menu_variations') }}

)

, menu_recipes as (

    select * from {{ ref('pim__menu_recipes') }}

)

, recipes as (

    select * from {{ ref('pim__recipes') }}

)

, recipe_portions as (

    select * from {{ ref('pim__recipe_portions') }}

)

, weekly_menus_variations_recipies_joined as (
    select
        weekly_menus.weekly_menu_id
        , weekly_menus.company_id
        , menus.menu_id
        , menu_recipes.menu_recipe_id
        , menu_variations.product_variation_id
        , menu_recipes.recipe_id
        , recipe_portions.recipe_portion_id

        -- added temporariliy for debugging purposes
        , menu_variations.portion_id as portion_id_variations
        , recipe_portions.portion_id as portion_id_recipes

        , weekly_menus.menu_year
        , weekly_menus.menu_week
        , weekly_menus.menu_week_monday_date

        , menu_variations.menu_number_days
        , menu_recipes.menu_recipe_order

        , weekly_menus.weekly_menu_status_code_id
        , menus.menu_status_code_id
        , recipes.recipe_status_code_id

    from weekly_menus
    left join menus
        on weekly_menus.weekly_menu_id = menus.weekly_menu_id
    left join menu_variations 
        on menus.menu_id = menu_variations.menu_id
    left join menu_recipes
        on menus.menu_id = menu_recipes.menu_id
        and menu_variations.menu_number_days >= menu_recipes.menu_recipe_order
    left join recipes
        on menu_recipes.recipe_id = recipes.recipe_id
    left join recipe_portions
        on recipes.recipe_id = recipe_portions.recipe_id
        and menu_variations.portion_id = recipe_portions.portion_id
)

select * from weekly_menus_variations_recipies_joined