with

menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, add_keys as (
    select
        md5(concat_ws('-',
            menu_weeks.weekly_menu_id,
            menu_weeks.menu_id,
            menu_weeks.product_variation_id,
            menu_weeks.recipe_id,
            menu_weeks.menu_recipe_id,
            coalesce(portions.portion_id, menu_weeks.portion_id_menus),
            menu_weeks.menu_number_days,
            menu_weeks.menu_recipe_order
        )) as pk_fact_menus
        , menu_weeks.weekly_menu_id
        , menu_weeks.company_id
        , menu_weeks.language_id
        , menu_weeks.menu_id
        , menu_weeks.menu_variation_id
        , menu_weeks.product_variation_id
        , menu_weeks.menu_recipe_id
        , menu_weeks.recipe_id

        , menu_weeks.recipe_portion_id
        , menu_weeks.portion_id_menus
        , portions.portion_id as portion_id_products
        , coalesce(portions.portion_id, menu_weeks.portion_id_menus) as portion_id

        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_monday_date
        , menu_weeks.menu_week_financial_date

        , menu_weeks.menu_number_days
        , menu_weeks.menu_recipe_order

        , menu_weeks.is_locked_recipe
        , menu_weeks.is_selected_menu
        , menu_weeks.is_dish
        , menu_weeks.is_future_menu_week

        {# FKS #}
        , md5(menu_weeks.company_id) as fk_dim_companies
        , cast(date_format(menu_weeks.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(
            concat(
                -- There are cases before 2023-05-08 where the portion_id from the product variations and the menu variations deviates.
                -- In those cases we will use the portion_id from the product variations.
                -- In cases where portions_products.portion_id is null we will use the portion_id from the menu variations.
                -- We will catch new cases from tests and try to get them fixed in the source systems.
                coalesce(
                    portions.portion_id
                    , menu_weeks.portion_id_menus
                )
            , menu_weeks.language_id)
        ) as fk_dim_portions
        , md5(concat(menu_weeks.product_variation_id, menu_weeks.company_id)) as fk_dim_products
        , coalesce(md5(cast(concat(menu_weeks.recipe_id, menu_weeks.language_id) as string)),0) as fk_dim_recipes

    from menu_weeks
    left join portions
        on menu_weeks.portion_name_products = portions.portion_name_local
        and menu_weeks.language_id = portions.language_id

)

select * from add_keys
