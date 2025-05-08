with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
    where basket_type_id = '{{ var("mealbox_basket_type_id") }}'

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, deviations_with_preselector as (

    select
        deviations.billing_agreement_id
        , deviations.billing_agreement_basket_id
        , deviations.company_id
        , products.product_type_id
        , products.product_id
        , products.meals
        , products.portions
        , deviations.product_variation_id
        , deviations.product_variation_quantity
        , deviations.deviation_created_at as valid_from
        , 'preselector deviation' as basket_source
    from deviations
    left join products
        on
            deviations.product_variation_id = products.product_variation_id
            and deviations.company_id = products.company_id
    -- only keep deviations with preselector
    where deviations.billing_agreement_basket_deviation_origin_id = '{{ var("preselector_origin_id") }}'

)

select * from deviations_with_preselector
