with

subscribed_products_list as (

    select * from {{ ref('cms__billing_agreement_basket_products_list') }}
)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, baskets_scd1 (

    select * from baskets
    where valid_to = '{{var("future_proof_date")}}'
)

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_basket_id' %}
{% set table_names = [
    'baskets' 
    , 'subscribed_products_list'
    ] %}

, scd2_tables_joined as (
    
    {{ join_scd2_tables(id_column, table_names) }}

)


, subscribed_products_exploded as (

    select
        scd2_tables_joined.billing_agreement_basket_id
        , basket_product_object.product_variation_id
        , basket_product_object.product_variation_quantity
        , basket_product_object.is_extra as is_extra_product
        , scd2_tables_joined.valid_from
        , scd2_tables_joined.valid_to
        , scd2_tables_joined.basket_source
    from scd2_tables_joined
        lateral view outer EXPLODE(basket_products_list) as basket_product_object

)

, adding_product_info_and_signup_at as (

    select
        subscribed_products_exploded.billing_agreement_basket_id
        , baskets_scd1.billing_agreement_id
        , billing_agreements.company_id
        , subscribed_products_exploded.product_variation_id
        , subscribed_products_exploded.product_variation_quantity
        , subscribed_products_exploded.is_extra_product
        , products.product_type_id
        , products.product_id
        , products.meals
        , products.portions
        , subscribed_products_exploded.valid_from
        , subscribed_products_exploded.valid_to
        , billing_agreements.signup_at
        , subscribed_products_exploded.basket_source
    from subscribed_products_exploded 
    left join baskets_scd1
        on subscribed_products_exploded.billing_agreement_basket_id = baskets_scd1.billing_agreement_basket_id
    left join billing_agreements
        on baskets_scd1.billing_agreement_id = billing_agreements.billing_agreement_id
    left join products
        on subscribed_products_exploded.product_variation_id = products.product_variation_id
        and billing_agreements.company_id = products.company_id

)

select * from adding_product_info_and_signup_at
