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

, portions as (

    select * from {{ ref('pim__portions') }}

)

, portion_translations as (

    select * from {{ ref('pim__portion_translations') }}

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, countries as (

    select * from {{ ref('cms__countries') }}

)

, weekly_menus_variations_recipes_portions_joined as (
    select
        weekly_menus.weekly_menu_id
        , weekly_menus.company_id
        , countries.language_id
        , menus.menu_id
        , menu_variations.menu_variation_id
        , menu_recipes.menu_recipe_id
        , menu_variations.product_variation_id
        , menu_recipes.recipe_id

        , recipe_portions.recipe_portion_id
        , menu_variations.portion_id
        , recipe_portions.portion_id as portion_id_recipes
        
        , weekly_menus.menu_year
        , weekly_menus.menu_week
        , weekly_menus.menu_week_monday_date

        , menu_variations.menu_number_days
        , menu_recipes.menu_recipe_order

        --TODO: Figure out were portion_size and portion_name belongs, in dim_products or dim_recipes.
        --      dim_products won't work because portions has 21,31,41 for danish +portions.
        --      portion_size from pim works better.
        , portions.portion_size as portion_quantity
        , portion_translations.portion_name

        , menus.is_selected_menu
        , menus.is_locked_recipe

    from weekly_menus
    left join menus
        on weekly_menus.weekly_menu_id = menus.weekly_menu_id
    left join menu_variations 
        on menus.menu_id = menu_variations.menu_id
    left join menu_recipes
        on menus.menu_id = menu_recipes.menu_id
        and menu_variations.menu_number_days >= menu_recipes.menu_recipe_order
    left join recipe_portions
        on menu_recipes.recipe_id = recipe_portions.recipe_id
        and menu_variations.portion_id = recipe_portions.portion_id
    left join portions
        on recipe_portions.portion_id = portions.portion_id
    left join companies
        on weekly_menus.company_id = companies.company_id
    left join countries
        on companies.country_id = countries.country_id
    left join portion_translations
        on recipe_portions.portion_id = portion_translations.portion_id
        and countries.language_id = portion_translations.language_id
)

select * from weekly_menus_variations_recipes_portions_joined