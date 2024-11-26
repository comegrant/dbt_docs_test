with

basket_products as (

    select * from {{ ref('cms__billing_agreement_basket_products_list') }}
)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)


, basket_products_exploded as (

    select
        basket_products.billing_agreement_basket_id
        , basket_product_object.product_variation_id
        , basket_product_object.product_variation_quantity
        , basket_product_object.is_extra as is_extra_product
        , basket_products.valid_from
        , basket_products.valid_to
    from basket_products
        lateral view EXPLODE(basket_products_list) as basket_product_object

)

, basket_products_with_extra_info as (

    select
        basket_products_exploded.billing_agreement_basket_id
        , baskets.billing_agreement_id
        , billing_agreements.company_id
        , basket_products_exploded.product_variation_id
        , basket_products_exploded.product_variation_quantity
        , basket_products_exploded.is_extra_product
        , products.product_type_id
        , products.product_id
        , products.meals
        , products.portions
        , basket_products_exploded.valid_from
        , basket_products_exploded.valid_to
        , billing_agreements.signup_at
    from basket_products_exploded 
    left join baskets
        on basket_products_exploded.billing_agreement_basket_id = baskets.billing_agreement_basket_id
    left join billing_agreements
        on baskets.billing_agreement_id = billing_agreements.billing_agreement_id
    left join products
        on basket_products_exploded.product_variation_id = products.product_variation_id
        and billing_agreements.company_id = products.company_id

)

select * from basket_products_with_extra_info
