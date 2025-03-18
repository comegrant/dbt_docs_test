with

menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)


, companies as (

    select * from {{ ref('dim_companies') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, dates as (

    select *
    from {{ ref('dim_dates') }}

)

, menu_year_and_week as (

    select distinct
        year_of_calendar_week as menu_year,
        calendar_week as menu_week
    from dates

)

, menu_weeks_with_flags as (
    select
        menu_weeks.*
        , case
            when menu_year_and_week.menu_week is null
            then true else false
            end as is_artificial_week
        , case
            when menu_weeks.recipe_id is not null
            then true
            else false
            end as has_menu_recipes
        , case
            when products.product_type_id in (
                    '2F163D69-8AC1-6E0C-8793-FF0000804EB3',
                    'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
                )
            then true
            else false
            end as is_dish
        , case
            when menu_weeks.recipe_portion_id is not null
            then true
            else false
            end as has_recipe_portions

        , products.portion_name as portion_name_products
    from menu_weeks
    left join menu_year_and_week
        on menu_weeks.menu_year = menu_year_and_week.menu_year
        and menu_weeks.menu_week = menu_year_and_week.menu_week
    left join products
        on menu_weeks.product_variation_id = products.product_variation_id
        and menu_weeks.company_id = products.company_id
)

, menu_weeks_with_flags_and_fks as (
    select
        md5(concat_ws('-',
            menu_weeks_with_flags.weekly_menu_id,
            menu_weeks_with_flags.menu_id,
            menu_weeks_with_flags.product_variation_id,
            menu_weeks_with_flags.recipe_id,
            menu_weeks_with_flags.recipe_portion_id,
            menu_weeks_with_flags.menu_recipe_id,
            menu_weeks_with_flags.portion_id,
            menu_weeks_with_flags.menu_number_days,
            menu_weeks_with_flags.menu_recipe_order,
            menu_weeks_with_flags.has_menu_recipes,
            menu_weeks_with_flags.has_recipe_portions
        )) as pk_fact_menus
        , menu_weeks_with_flags.weekly_menu_id
        , menu_weeks_with_flags.company_id
        , menu_weeks_with_flags.menu_id
        , menu_weeks_with_flags.menu_variation_id
        , menu_weeks_with_flags.product_variation_id
        , menu_weeks_with_flags.menu_recipe_id
        , menu_weeks_with_flags.recipe_id

        , menu_weeks_with_flags.recipe_portion_id
        , menu_weeks_with_flags.portion_id as portion_id_menus
        , menu_weeks_with_flags.portion_id_recipes
        , portions_products.portion_id as portion_id_products
        , coalesce(portions_products.portion_id, menu_weeks_with_flags.portion_id) as portion_id
        , menu_weeks_with_flags.portion_quantity
        , menu_weeks_with_flags.portion_name

        , menu_weeks_with_flags.menu_year
        , menu_weeks_with_flags.menu_week
        , menu_weeks_with_flags.menu_week_monday_date

        , menu_weeks_with_flags.menu_number_days
        , menu_weeks_with_flags.menu_recipe_order

        , menu_weeks_with_flags.is_locked_recipe
        , menu_weeks_with_flags.is_selected_menu
        , menu_weeks_with_flags.has_menu_recipes
        , menu_weeks_with_flags.has_recipe_portions
        , menu_weeks_with_flags.is_dish
        , menu_weeks_with_flags.is_artificial_week

        {# FKS #}
        , md5(cast(concat(menu_weeks_with_flags.recipe_id, companies.language_id) as string)) as fk_dim_recipes
        , md5(concat(menu_weeks_with_flags.product_variation_id, companies.company_id)) as fk_dim_products
        , md5(
            concat(
                -- There are cases before 2023-05-08 where the portion_id from the product variations and the menu variations deviates.
                -- In those cases we will use the portion_id from the product variations.
                -- In cases where portions_products.portion_id is null we will use the portion_id from the menu variations.
                -- We will catch new cases from tests and try to get them fixed in the source systems
                coalesce(
                    portions_products.portion_id
                    ,menu_weeks_with_flags.portion_id
                )
            , companies.language_id)
        ) as fk_dim_portions
        , cast(date_format(menu_weeks_with_flags.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(menu_weeks_with_flags.company_id) as fk_dim_companies

    from menu_weeks_with_flags
    left join companies
        on menu_weeks_with_flags.company_id = companies.company_id
    left join portions as portions_products
        on menu_weeks_with_flags.portion_name_products = portions_products.portion_name_local
        and companies.language_id = portions_products.language_id

)

select * from menu_weeks_with_flags_and_fks
