with

basket_products as (

    select * from {{ref('int_basket_products_scd2')}}

)

, dim_billing_agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

, dim_products as (

    select * from {{ref('dim_products')}}

)

, joined as (

    select
    distinct
    md5(concat(
        cast(dim_billing_agreements.pk_dim_billing_agreements as string)
        , cast(basket_products.product_variation_id as string)
        , cast(basket_products.company_id as string)
        )
    ) as pk_bridge_billing_agreements_basket_products
    , dim_billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
    , dim_products.pk_dim_products as fk_dim_products
    , basket_products.billing_agreement_basket_product_updated_id
    , basket_products.product_variation_id
    , basket_products.company_id
    , basket_products.product_variation_quantity
    from
    basket_products
    left join dim_billing_agreements
    on basket_products.billing_agreement_basket_product_updated_id = dim_billing_agreements.billing_agreement_basket_product_updated_id
    left join dim_products
    on dim_products.product_variation_id = basket_products.product_variation_id
    and dim_products.company_id = basket_products.company_id
    where dim_billing_agreements.pk_dim_billing_agreements is not null
    and dim_products.pk_dim_products is not null

)

select * from joined