with

source as (

    select * from {{ source('forecasting', 'forecast_orders') }}

)


, renamed as (

    select
        {# ids #}
        forecast_orders_id
        , forecast_job_run_metadata_id
        , company_id
        , forecast_group_id
        , forecast_model_id

        {# ints #}
        , menu_year
        , menu_week
        , forecast_order_quantity as order_quantity_forecast

        {# system #}
        , created_at as source_created_at

    from source

)

select * from renamed
