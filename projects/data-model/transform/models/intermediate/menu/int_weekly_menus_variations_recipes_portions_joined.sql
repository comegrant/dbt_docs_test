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

, recipe_portions as (

    select * from {{ ref('pim__recipe_portions') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, countries as (

    select * from {{ ref('cms__countries') }}

)

, dates as (

    select * from {{ ref('data_platform__dates') }}

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
        , menu_variations.portion_id as portion_id_menus
        , products.portion_name as portion_name_products

        , weekly_menus.menu_year
        , weekly_menus.menu_week
        , weekly_menus.menu_week_monday_date
        , {{ get_financial_date_from_monday_date(' weekly_menus.menu_week_monday_date') }} as menu_week_financial_date

        , menu_variations.menu_number_days
        , menu_recipes.menu_recipe_order

        , menus.is_selected_menu
        , menus.is_locked_recipe

        , products.is_dish
        , products.is_grocery
        , products.is_mealbox
        , dates.is_future_menu_week

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
    left join companies
        on weekly_menus.company_id = companies.company_id
    left join countries
        on companies.country_id = countries.country_id
    left join products
        on menu_variations.product_variation_id = products.product_variation_id
        and weekly_menus.company_id = products.company_id
    -- only include years and weeks that exist in the calendar
    -- week 53 in year where week 53 does not exist has been used
    -- to create test earlier
    inner join dates
        on weekly_menus.menu_year = dates.year_of_calendar_week
        and weekly_menus.menu_week = dates.calendar_week
        and dates.day_of_week = 1

    where weekly_menus.menu_year >= 2019
        and menus.is_selected_menu = true
        and (
                --this is to include all menu variation that has no recipe
                --i.e. most Standalone groceries or similar products
                menu_recipes.recipe_id is null
                or (
                    --this is for all menu variations with recipes,
                    --to only include the menu variations where the recipe variation actually exists
                    menu_recipes.recipe_id is not null
                        and recipe_portions.portion_id is not null
                )

        )
)

select * from weekly_menus_variations_recipes_portions_joined
