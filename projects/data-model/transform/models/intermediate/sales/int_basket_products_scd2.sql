with 

baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}

)

, basket_products as (

    select * from {{ ref('cms__billing_agreement_basket_products_list') }}
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

        md5(concat(
            cast(billing_agreement_basket_id as string)
            , cast(valid_from as string)
        )) as billing_agreement_basket_product_updated_id
        , billing_agreement_basket_id
        , basket_products_list
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

, explode_products as (

    select 
    billing_agreement_basket_product_updated_id
    , billing_agreement_basket_id
    , company_id
    , billing_agreement_id
    , shipping_address_id
    , basket_delivery_week_type_id
    , timeblock_id
    , is_default_basket
    , is_active_basket
    , basket_product_object.product_variation_id
    , basket_product_object.product_variation_quantity
    , basket_product_object.is_extra as is_extra_product
    , valid_from
    , valid_to

    from add_scd1
    lateral view explode(basket_products_list) as basket_product_object

)

select * from explode_products