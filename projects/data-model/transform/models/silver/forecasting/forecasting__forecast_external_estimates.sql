with

source as (

    select * from {{ source('forecasting', 'forecast_external_estimates') }}
    where deleted = 0 -- should always be 0 but just to ensure the table does not contain any deleted values

)

, rename_and_add_group_and_model_ids as (

    select
        {# ids #}
        company_id
        , '3F055227-1EA8-4083-A95C-839B1670C49B' as forecast_group_id
        , 'EF8164BB-3CF5-4C1E-8CF0-52E1431FC6CE' as forecast_model_id
        , variation_id as product_variation_id

        {# ints #}
        , year as menu_year
        , week as menu_week
        , quantity as product_variation_quantity_forecast

        {# system #}
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from rename_and_add_group_and_model_ids
