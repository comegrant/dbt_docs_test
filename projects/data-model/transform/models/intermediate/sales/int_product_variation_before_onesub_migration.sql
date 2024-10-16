
with 

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, deviations_preselector as (
    
    select distinct
        billing_agreement_basket_id  
        , menu_week_monday_date
    from deviations
    where billing_agreement_basket_deviation_origin_id = '{{ var("preselector_origin_id") }}'

)

, onesub_variations as (

    select distinct
        product_variation_id
    from products
    where product_id = '{{ var("onesub_product_id") }}'

)

, deviations_onesub_no_preselector as (
    select distinct
        deviations.billing_agreement_basket_id
        , deviations.menu_week_monday_date
        , max_by(deviations.product_variation_id, deviations.source_created_at) as product_variation_id
    from deviations
    left anti join deviations_preselector
        on deviations.billing_agreement_basket_id = deviations_preselector.billing_agreement_basket_id
        and deviations.menu_week_monday_date = deviations_preselector.menu_week_monday_date
    where product_variation_id in (select product_variation_id from onesub_variations)
    group by 1,2
)

, pre_onesub_mealbox_product_variations as (

    select distinct
        product_variation_id
    from products
    where product_type_id in ('{{ var("mealbox_product_type_id") }}', '{{ var("financial_product_type_id") }}')
    and product_id != '{{ var("onesub_product_id") }}'

)

-- Assumes that there are no more than one mealbox product variation per deviation
, product_variation_before_onesub as (

    select 
        deviations_onesub_no_preselector.billing_agreement_basket_id
        , deviations_onesub_no_preselector.menu_week_monday_date
        , deviations_onesub_no_preselector.product_variation_id
        , min_by(deviations.product_variation_id, deviations.source_created_at) as pre_onesub_product_variation_id
    from deviations_onesub_no_preselector
    left join deviations
        on deviations_onesub_no_preselector.billing_agreement_basket_id = deviations.billing_agreement_basket_id
        and deviations_onesub_no_preselector.menu_week_monday_date = deviations.menu_week_monday_date
        and deviations.product_variation_id in (select product_variation_id from pre_onesub_mealbox_product_variations)
    group by 1,2,3

)

select * from product_variation_before_onesub