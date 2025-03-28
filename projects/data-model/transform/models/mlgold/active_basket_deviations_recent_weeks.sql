with deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, recipes as (

    select * from {{ ref('dim_recipes') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, companies as (

    select * from {{ ref('dim_companies') }}

)

, join_deviations_and_recipes as (

    select distinct
        deviations.billing_agreement_id
        , deviations.company_id
        , deviations.menu_week_monday_date
        , deviations.menu_week
        , deviations.menu_year
        , recipes.main_recipe_id
        , recipes.recipe_name
        , deviations.deviation_created_at
    from deviations
    left join companies
        on deviations.company_id = companies.company_id
    left join menus
        on
            deviations.menu_week_monday_date = menus.menu_week_monday_date
            and deviations.product_variation_id = menus.product_variation_id
            and deviations.company_id = menus.company_id
    left join products
        on
            deviations.product_variation_id = products.product_variation_id
            and deviations.company_id = products.company_id
    left join recipes
        on menus.recipe_id = recipes.recipe_id
        and companies.language_id = recipes.language_id
    where products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
    -- Only include the current active deviations
    and deviations.is_active_deviation = true
    -- Only includes the previous 2 months of menu weeks and all future menu weeks
    and deviations.menu_week_monday_date >= date_add(current_date, -60)
)

select * from join_deviations_and_recipes
