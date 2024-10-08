{{
    config(
        materialized='incremental',
        unique_key='billing_agreement_basket_deviation_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_basket_deviation') }}

)

, renamed as (

    select

        
        {# ids #}
        id as billing_agreement_basket_deviation_id
        , billing_agreement_basket_id
        , origin as billing_agreement_basket_deviation_origin_id
        , order_delivery_type as delivery_week_type_id

        {# numerics #}
        , year as menu_year
        , week as menu_week
        
        {# booleans #}
        , is_active as is_active_deviation

        {# date #}
        , {{ get_iso_week_start_date('year', 'week') }} as menu_week_monday_date
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
