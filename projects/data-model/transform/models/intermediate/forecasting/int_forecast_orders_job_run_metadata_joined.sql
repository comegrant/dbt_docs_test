with

job_run_metadata as (

    select * from {{ ref('forecasting__forecast_job_run_metadata') }}

)

, forecast_orders as (

    select * from {{ ref('forecasting__forecast_orders') }}

)


, forecast_orders_metadata_joined_filtered as (

    select
        forecast_orders.forecast_orders_id
        , forecast_orders.forecast_job_run_metadata_id
        , job_run_metadata.forecast_job_run_id
        , job_run_metadata.forecast_job_id
        , forecast_orders.company_id
        , forecast_orders.forecast_group_id
        , forecast_orders.forecast_model_id
        , forecast_orders.menu_year
        , forecast_orders.menu_week
        , job_run_metadata.menu_week_monday_date
        , forecast_orders.order_quantity_forecast
        , job_run_metadata.horizon_index
        , job_run_metadata.forecast_horizon
        , job_run_metadata.is_most_recent_for_menu_week_and_horizon_index
        , job_run_metadata.is_most_recent_for_menu_week
        , forecast_orders.source_created_at

    from forecast_orders

    left join job_run_metadata
        on forecast_orders.forecast_job_run_metadata_id = job_run_metadata.forecast_job_run_metadata_id

    where job_run_metadata.is_most_recent_for_menu_week_and_horizon_index is true

)

select * from forecast_orders_metadata_joined_filtered
