with

source as (

    select * from {{ source('forecasting', 'forecast_variations') }}

)

, renamed as (

    select
        {# ids #}
        forecast_variations_id
        , forecast_job_run_metadata_id
        , company_id
        , forecast_group_id
        , forecast_model_id
        , product_variation_id

        {# ints #}
        , menu_year
        , menu_week
        , forecast_variation_quantity as product_variation_quantity_forecast_analytics

        {# system #}
        , created_at as source_created_at

    from source

)

select * from renamed
