with

order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, baskets as (
    select * from {{ ref('cms__billing_agreement_baskets') }}
    where valid_to = '{{var("future_proof_date")}}'
)

, mealboxes_from_orders as (
    select 
        order_lines.billing_agreement_id
        , baskets.billing_agreement_basket_id
        , billing_agreements.company_id
        , products.product_id
        , products.meals
        , products.portions
        , order_lines.product_variation_id
        , order_lines.source_created_at as valid_from
    from order_lines
    left join billing_agreements
        on order_lines.billing_agreement_id = billing_agreements.billing_agreement_id
    left join products
        on order_lines.product_variation_id = products.product_variation_id
        and billing_agreements.company_id = products.company_id
    left join baskets
        on order_lines.billing_agreement_id = baskets.billing_agreement_id
    where products.product_type_id = '{{ var("mealbox_product_type_id") }}'
    and products.product_id != '{{ var("onesub_product_id") }}'
    and order_lines.order_type_id in (
        '1C182E51-ECFA-4119-8928-F2D9F57C5FCC', --Orders after cutoff
        '5F34860B-7E61-46A0-80F7-98DCDC53BA9E', --Recurring
        'C7D2684C-B715-4C6C-BF90-053757926679'  --Order After Registration
    )

)

select * from mealboxes_from_orders
