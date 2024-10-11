with 

baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}

)

, basket_products as (

    select * from {{ ref('cms__billing_agreement_basket_products') }}
)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, billing_agreements_scd1 as (
    
    select 
        billing_agreement_id, 
        company_id 
    from billing_agreements
    where valid_to is null

)

, baskets_scd2 as (
    select

        billing_agreement_basket_id
        , billing_agreement_id
        , shipping_address_id
        , basket_delivery_week_type_id
        , timeblock_id
        , is_default_basket
        , is_active_basket
        , valid_from
        , valid_to

    from baskets
)

, basket_products_scd2 as (

    select

        billing_agreement_basket_product_id
        , billing_agreement_basket_id
        , product_variation_id
        , product_delivery_week_type_id

        {# numerics #}
        , product_variation_quantity
        
        {# booleans #}
        , is_extra_product
        
        {# scd #}
        , valid_from
        , valid_to
        

    from basket_products

)

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_basket_id' %}
{% set table_names = [
    'baskets_scd2', 
    'basket_products_scd2'
    ] %}

, scd2_tables_joined as (
    
    {{ join_scd2_tables(id_column, table_names) }}

)


, add_scd1 as (

    select
        billing_agreements_scd1.company_id
       , scd2_tables_joined.*
    from scd2_tables_joined
    left join billing_agreements_scd1
        on scd2_tables_joined.billing_agreement_id = billing_agreements_scd1.billing_agreement_id
)

select * from add_scd1