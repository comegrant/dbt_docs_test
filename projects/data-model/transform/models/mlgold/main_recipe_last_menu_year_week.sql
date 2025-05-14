with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, menus as (

    select * from {{ ref('fact_menus') }}

)

, recipes as (

    select * from {{ ref('dim_recipes') }}
)

, deviations_filter_active as (

    select deviations.*
    from deviations
    where is_active_deviation

)

, deviations_add_fks as (

    select
        deviations_filter_active.*
        , md5(concat(product_variation_id, company_id))               as fk_dim_products
        , cast(date_format(menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
    from deviations_filter_active

)

, deviations_menus_recipes_joined as (

    select
        deviations_add_fks.billing_agreement_id
        , deviations_add_fks.company_id
        , deviations_add_fks.menu_year * 100 + deviations_add_fks.menu_week as menu_year_week
        , coalesce(recipes.main_recipe_id, recipes.recipe_id)               as main_recipe_id
    from deviations_add_fks
    left join menus
        on
            deviations_add_fks.fk_dim_products = menus.fk_dim_products
            and deviations_add_fks.fk_dim_date = menus.fk_dim_dates
    left join recipes
        on menus.fk_dim_recipes = recipes.pk_dim_recipes

)

, main_recipe_last_chosen as (

    select
        billing_agreement_id
        , company_id
        , main_recipe_id
        , max(menu_year_week) as most_recent_menu_year_week
    from deviations_menus_recipes_joined
    group by all

)

select * from main_recipe_last_chosen
