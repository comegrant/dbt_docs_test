with latest_forecast as (
    select *
    from {{ ref('fact_forecast_variations') }}
    where
        order_quantity_forecast is not null
        and is_most_recent_menu_week_forecast
        and forecast_group_id in (
            '189D4373-E78D-46E7-B770-6FA58E170223' --flex
            , 'E4B05BA4-8553-4321-BEDB-01BFD2F13C84' --non-flex
        )
    order by menu_year, menu_week
)

, flex_forecasts as (
    select
        forecast_job_run_id
        , company_id
        , menu_year
        , menu_week
        , order_quantity_forecast as forecast_flex_orders
    from
        latest_forecast
    where forecast_group_id = '189D4373-E78D-46E7-B770-6FA58E170223'
)

, total_orders as (
    select
        forecast_job_run_id
        , company_id
        , menu_year
        , menu_week
        , sum(order_quantity_forecast) as forecast_total_orders
    from
        latest_forecast
    group by
        forecast_job_run_id
        , company_id
        , menu_year
        , menu_week
)

select
    total_orders.*
    , forecast_flex_orders
    , forecast_flex_orders / forecast_total_orders as forecast_flex_share
from total_orders
left join flex_forecasts
    on
        total_orders.company_id = flex_forecasts.company_id
        and total_orders.menu_year = flex_forecasts.menu_year
        and total_orders.menu_week = flex_forecasts.menu_week
        and total_orders.forecast_job_run_id = flex_forecasts.forecast_job_run_id
order by menu_year, menu_week, company_id
