with

forecast_orders as (

    select * from {{ ref('int_forecast_orders_job_run_metadata_joined') }}

)

, forecast_variations as (

    select * from {{ ref('int_forecast_variations_metadata_external_estimates_joined') }}

)

, weekly_menus_variations as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, ingredient_combinations as (

    select * from {{ ref('int_recipes_with_ingredient_combinations') }}

)


, forecast_orders_forecast_variations_unioned as (

    select

        forecast_job_run_id
        , forecast_job_id
        , menu_year
        , menu_week
        , menu_week_monday_date
        , company_id
        , forecast_group_id
        , forecast_model_id
        , forecast_horizon_index
        , forecast_horizon

        -- variations
        , product_variation_id
        , product_variation_quantity_forecast_analytics
        , product_variation_quantity_forecast

        -- orders
        -- there is no forecast order quantity in forecast_variations
        , null as order_quantity_forecast

        , source_created_at as forecast_generated_at
        , is_most_recent_menu_week_horizon_forecast
        , is_most_recent_menu_week_forecast

    from forecast_variations

    union all

    select

        forecast_job_run_id
        , forecast_job_id
        , menu_year
        , menu_week
        , menu_week_monday_date
        , company_id
        , forecast_group_id
        , forecast_model_id
        , forecast_horizon_index
        , forecast_horizon

        -- variations
        -- Not relevant product since this table contains the order forecast, which does not relate to any products
        , '0' as product_variation_id
        , null as product_variation_quantity_forecast_analytics
        , null as product_variation_quantity_forecast

        -- orders
        , order_quantity_forecast

        , source_created_at as forecast_generated_at
        , is_most_recent_menu_week_horizon_forecast
        , is_most_recent_menu_week_forecast

    from forecast_orders

)


, add_recipe_info as (

    select

        forecast_orders_forecast_variations_unioned.*
        , weekly_menus_variations.language_id
        , weekly_menus_variations.is_dish
        , weekly_menus_variations.recipe_id
        , portions.portion_id
        , ingredient_combinations.ingredient_combination_id

    from forecast_orders_forecast_variations_unioned

    left join weekly_menus_variations
        on forecast_orders_forecast_variations_unioned.company_id = weekly_menus_variations.company_id
        and forecast_orders_forecast_variations_unioned.menu_year = weekly_menus_variations.menu_year
        and forecast_orders_forecast_variations_unioned.menu_week = weekly_menus_variations.menu_week
        and forecast_orders_forecast_variations_unioned.product_variation_id = weekly_menus_variations.product_variation_id

    left join products
        on forecast_orders_forecast_variations_unioned.product_variation_id = products.product_variation_id
        and forecast_orders_forecast_variations_unioned.company_id = products.company_id

    left join portions
        on products.portion_name = portions.portion_name_local
        and weekly_menus_variations.language_id = portions.language_id

    left join ingredient_combinations
        on weekly_menus_variations.recipe_id = ingredient_combinations.recipe_id
        and portions.portion_id = ingredient_combinations.portion_id
        and weekly_menus_variations.language_id = ingredient_combinations.language_id

)

, add_keys as (

    select
        md5(
            concat(
                menu_year
                , menu_week
                , company_id
                , forecast_group_id
                , forecast_model_id
                , product_variation_id
                , forecast_job_run_id
                , forecast_generated_at
            )
        ) as pk_fact_forecast_variations

    , forecast_job_run_id
    , forecast_job_id
    , menu_year
    , menu_week
    , company_id
    , forecast_group_id
    , forecast_model_id
    , forecast_horizon_index
    , forecast_horizon
    , language_id
    , recipe_id
    , portion_id
    , ingredient_combination_id

    -- variation forecasts
    , product_variation_id
    , product_variation_quantity_forecast_analytics
    , product_variation_quantity_forecast

    -- order forecasts
    , order_quantity_forecast

    , forecast_generated_at
    , is_most_recent_menu_week_horizon_forecast
    , is_most_recent_menu_week_forecast

    , md5(
        concat(
            forecast_job_id
            , company_id
            , forecast_group_id
            , forecast_model_id
            , forecast_horizon_index
            , forecast_horizon
            )
    ) as fk_dim_forecast_runs
    , md5(company_id) as fk_dim_companies
    , case 
        when product_variation_id = '0' 
        then '0'
        else md5(
            concat(
                product_variation_id,
                company_id)
            ) 
    end as fk_dim_products
    , cast(date_format(menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates_menu_week
    , cast(date_format(forecast_generated_at, 'yyyyMMdd') as int) as fk_dim_dates_forecast_generated_at
    , coalesce(
        md5(cast(concat(recipe_id, language_id) as string))
        , '0'
    ) as fk_dim_recipes
    , coalesce(
        md5(concat(portion_id, language_id))
        , '0'
    ) as fk_dim_portions
    , coalesce(
        md5(concat(ingredient_combination_id, language_id))
        , '0'
    ) as fk_dim_ingredient_combinations

    from add_recipe_info

)

select * from add_keys
