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
        , is_extra
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_by as source_updated_at
        , updated_at as source_updated_by

    from source

)

select * from renamed
