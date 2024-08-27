{{
    config(
        materialized='incremental',
        unique_key='order_line_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_order_line') }}

),

renamed as (

    select
        {# ids #}
        id as order_line_id
        , agreement_order_id as cms_order_id
        , variation_id as product_variation_id
     
        {# strings #}
        , initcap(typeOfLine) as order_line_type_name
 
        {# numerics #}
        , price as unit_price_ex_vat
        , (CAST(VAT as decimal (6, 4)) / 100) as vat
        , variation_qty

    from source

),

calculated_columns as (

    select
        {# ids #}
        order_line_id
        , cms_order_id
        , product_variation_id
     
        {# strings #}
        , order_line_type_name
 
        {# numerics #}
        , variation_qty
        , vat
        , unit_price_ex_vat
        , unit_price_ex_vat * (1+vat) as unit_price_inc_vat
        , unit_price_ex_vat * variation_qty as total_amount_ex_vat
        , unit_price_ex_vat * (1+vat) * variation_qty as total_amount_inc_vat

    from renamed

)

select * from calculated_columns