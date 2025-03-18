with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, deviations_onesub_migration as (

    select
        deviations.billing_agreement_basket_deviation_id
        , deviations.billing_agreement_id
        , deviations.billing_agreement_basket_id
        , deviations.company_id
        , products.product_id
        , products.meals
        , products.portions
        , deviations.product_variation_id
        , case
            when
                deviations.billing_agreement_basket_deviation_origin_id
                = '{{ var("mealselector_origin_id") }}'
                then deviations.deviation_created_at
            when deviations.billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}'
                then deviations.deviation_product_updated_at
        end as valid_from
    from deviations
    -- only include rows with onesub products
    inner join products
        on
            deviations.product_variation_id = products.product_variation_id
            and deviations.company_id = products.company_id
            and product_id = '{{ var("onesub_product_id") }}'
    -- only include deviations related to the onesub migration
    where 
    (
        (
            deviations.billing_agreement_basket_deviation_origin_id = '{{ var("mealselector_origin_id") }}'
            and deviation_created_by = 'Tech - Weekly menu'
        )
        or 
        (
            deviations.billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}'
            and deviation_product_updated_by = 'Tech - Script'
        )
    )
    and deviation_product_updated_at > '2024-09-25' 
    and deviation_product_updated_at < '2024-11-08'

)

select * from deviations_onesub_migration