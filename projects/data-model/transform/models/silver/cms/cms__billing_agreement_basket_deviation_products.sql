{{
    config(
        materialized='incremental',
        unique_key='billing_agreement_basket_deviation_product_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_basket_deviation_product') }}

)

, renamed as (

    select

        
        {# ids #}
        id as billing_agreement_basket_deviation_product_id
        , billing_agreement_basket_deviation_id
        , subscribed_product_variation_id as product_variation_id
        
        {# numerics #}
        , quantity as product_variation_quantity
        
        {# booleans #}
        , is_extra as is_extra_product
        
        {# system #}
        , case
            -- Preselector uses UTC as long as it is created by Deviation service
            -- Manual scripts done by cms is using UTC
            -- otherwise it is local time and must be converted to UTC 
            when created_by = 'Deviation service' or updated_by like '%Tech%'
                then created_at
            else 
                convert_timezone('Europe/Oslo', 'UTC', created_at)
        end as source_created_at

        , created_by as source_created_by
        
        , case
            -- Preselector uses UTC as long as it is created by Deviation service
            -- Manual scripts done by cms is using UTC
            -- otherwise it is local time and must be converted to UTC 
            when updated_by = 'Deviation service' or updated_by like '%Tech%'
                then updated_at
            else 
                convert_timezone('Europe/Oslo', 'UTC', updated_at)
        end as source_updated_at

    from source

)

select * from renamed
