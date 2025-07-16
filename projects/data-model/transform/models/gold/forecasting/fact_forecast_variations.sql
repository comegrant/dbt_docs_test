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

    from forecast_orders_forecast_variations_unioned

    left join weekly_menus_variations
        on forecast_orders_forecast_variations_unioned.company_id = weekly_menus_variations.company_id
        and forecast_orders_forecast_variations_unioned.menu_year = weekly_menus_variations.menu_year
        and forecast_orders_forecast_variations_unioned.menu_week = weekly_menus_variations.menu_week
        and forecast_orders_forecast_variations_unioned.product_variation_id = weekly_menus_variations.product_variation_id

)

, add_keys as (

    select
        md5(
            concat(
                add_recipe_info.menu_year
                , add_recipe_info.menu_week
                , add_recipe_info.company_id
                , add_recipe_info.forecast_group_id
                , add_recipe_info.forecast_model_id
                , add_recipe_info.product_variation_id
                , add_recipe_info.forecast_job_run_id
                , add_recipe_info.forecast_generated_at
            )
        ) as pk_fact_forecast_variations

    , add_recipe_info.forecast_job_run_id
    , add_recipe_info.forecast_job_id
    , add_recipe_info.menu_year
    , add_recipe_info.menu_week
    , add_recipe_info.company_id
    , add_recipe_info.forecast_group_id
    , add_recipe_info.forecast_model_id
    , add_recipe_info.forecast_horizon_index
    , add_recipe_info.forecast_horizon

    -- variation forecasts
    , add_recipe_info.product_variation_id
    , add_recipe_info.product_variation_quantity_forecast_analytics
    , add_recipe_info.product_variation_quantity_forecast

    -- order forecasts
    , add_recipe_info.order_quantity_forecast

    , add_recipe_info.forecast_generated_at
    , add_recipe_info.is_most_recent_menu_week_horizon_forecast
    , add_recipe_info.is_most_recent_menu_week_forecast

    , md5(
        concat(
            add_recipe_info.forecast_job_id
            , add_recipe_info.company_id
            , add_recipe_info.forecast_group_id
            , add_recipe_info.forecast_model_id
            , add_recipe_info.forecast_horizon_index
            , add_recipe_info.forecast_horizon
            )
    ) as fk_dim_forecast_runs
    , md5(add_recipe_info.company_id) as fk_dim_companies
    , case 
        when product_variation_id = '0' 
        then '0'
        else md5(
            concat(
                add_recipe_info.product_variation_id,
                add_recipe_info.company_id)
            ) 
    end as fk_dim_products
    , cast(date_format(add_recipe_info.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates_menu_week
    , cast(date_format(add_recipe_info.forecast_generated_at, 'yyyyMMdd') as int) as fk_dim_dates_forecast_generated_at
    , coalesce(
        md5(cast(concat(add_recipe_info.recipe_id, add_recipe_info.language_id) as string))
        , '0'
    ) as fk_dim_recipes

    from add_recipe_info

)

select * from add_keys
