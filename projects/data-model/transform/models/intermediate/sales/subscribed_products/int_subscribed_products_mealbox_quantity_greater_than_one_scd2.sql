with

subscribed_mealbox_products as (

    select * from {{ ref('int_subscribed_products_scd2') }}

)

, filter_mealbox_products_with_quantity_greater_than_one as (

    select * from subscribed_mealbox_products
    where product_variation_quantity > 1
    and is_mealbox = true

)

select * from filter_mealbox_products_with_quantity_greater_than_one