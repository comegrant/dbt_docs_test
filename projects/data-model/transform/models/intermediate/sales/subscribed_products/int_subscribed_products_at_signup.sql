with

basket_registered as (

    select * from {{ref('cms__register_info_baskets')}}

)

, products_registered as (

    select * from {{ref('cms__register_info_products')}}

)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, basket_and_products_joined as (
    select 
    basket_registered.billing_agreement_id
    , baskets.billing_agreement_basket_id
    , products.product_type_id
    , products.product_id
    , products.meals
    , products.portions
    , products_registered.product_variation_id
    , agreements.company_id
    , agreements.signup_at as valid_from
    , {{ get_scd_valid_to() }} as valid_to
    , 'signup basket' as basket_source
    from basket_registered
    left join products_registered
        on products_registered.register_info_basket_id = basket_registered.register_info_basket_id
    left join baskets
        on basket_registered.billing_agreement_id = baskets.billing_agreement_id
    left join agreements
        on basket_registered.billing_agreement_id = agreements.billing_agreement_id
    left join products
        on products_registered.product_variation_id = products.product_variation_id
        and agreements.company_id = products.company_id

)

select * from basket_and_products_joined