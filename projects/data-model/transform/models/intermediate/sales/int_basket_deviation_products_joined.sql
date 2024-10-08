with 

baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}

)

, deviations as (

    select * from {{ ref('cms__billing_agreement_basket_deviations') }}

)

, products as (

    select * from {{ ref('cms__billing_agreement_basket_deviation_products') }}

)

, baskets_distinct (
    select distinct 
        billing_agreement_basket_id
        , billing_agreement_id 
    from baskets
)

, basket_deviation_products_joined as (

    select
        baskets_distinct.billing_agreement_id
        , baskets_distinct.billing_agreement_basket_id
        , deviations.billing_agreement_basket_deviation_id
        , deviations.billing_agreement_basket_deviation_origin_id
        , products.billing_agreement_basket_deviation_product_id
        , products.product_variation_id
        , deviations.delivery_week_type_id
        , deviations.menu_week_monday_date
        , deviations.menu_year
        , deviations.menu_week
        , products.product_variation_quantity
        , deviations.is_active_deviation
        , products.is_extra_product
        , deviations.source_created_at
    from deviations
    left join products
      on deviations.billing_agreement_basket_deviation_id = products.billing_agreement_basket_deviation_id
    left join baskets_distinct
      on deviations.billing_agreement_basket_id = baskets_distinct.billing_agreement_basket_id
)

select * from basket_deviation_products_joined