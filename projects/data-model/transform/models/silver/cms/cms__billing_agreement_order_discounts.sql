{{
    config(
        materialized='incremental',
        unique_key='billing_agreement_order_discount_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_order_discount') }}

)

, renamed as (

    select
        
        {# ids #}
        relation_id                 as billing_agreement_order_discount_id
        , order_id                  as ops_order_id
        , discount_id
        , discount_criteria_id
        , cancelled_relation_id     as discount_order_cancelled_relation_id
        , coupon_id                 as discount_coupon_code_id
        , order_line                as billing_agreement_order_line_id
        
        {# system #}
        , created_by                as system_created_by
        , created_at                as system_created_at

    from source

)

select * from renamed
