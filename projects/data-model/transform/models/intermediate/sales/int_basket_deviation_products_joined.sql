with 

baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, deviations as (

    select * from {{ ref('cms__billing_agreement_basket_deviations') }}

)

, deviation_products as (

    select * from {{ ref('cms__billing_agreement_basket_deviation_products') }}

)

, basket_deviation_products_joined as (

    select
        baskets.billing_agreement_id
        , baskets.billing_agreement_basket_id
        , billing_agreements.company_id
        , deviations.billing_agreement_basket_deviation_id
        , deviations.billing_agreement_basket_deviation_origin_id
        , deviation_products.billing_agreement_basket_deviation_product_id
        , deviation_products.product_variation_id
        , deviations.delivery_week_type_id
        , deviations.menu_week_monday_date
        , deviations.menu_year
        , deviations.menu_week
        , deviation_products.product_variation_quantity
        , deviations.is_active_deviation
        , deviation_products.is_extra_product
        , deviations.source_created_at as deviation_created_at
        , deviations.source_created_by as deviation_created_by
        , deviations.source_updated_at as deviation_updated_at
        , deviations.source_updated_by as deviation_updated_by
        , deviation_products.source_created_at as deviation_product_created_at
        , deviation_products.source_created_by as deviation_product_created_by
        , deviation_products.source_updated_at as deviation_product_updated_at
        , deviation_products.source_updated_by as deviation_product_updated_by
    from deviations
    left join deviation_products
      on deviations.billing_agreement_basket_deviation_id = deviation_products.billing_agreement_basket_deviation_id
    left join baskets
      on deviations.billing_agreement_basket_id = baskets.billing_agreement_basket_id
    left join billing_agreements
        on billing_agreements.billing_agreement_id = baskets.billing_agreement_id 
)

select * from basket_deviation_products_joined