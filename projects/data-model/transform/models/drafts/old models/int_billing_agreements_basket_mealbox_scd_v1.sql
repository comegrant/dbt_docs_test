with 

basket_products as (

    select * from {{ ref('int_basket_products_scd2') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, basket_products_filter_and_join_mealbox as (

    select 
        basket_products.billing_agreement_id
        , basket_products.valid_from
        , basket_products.valid_to
        , products.product_name
        , case 
            when products.product_id = 'D699150E-D2DE-4BC1-A75C-8B70C9B28AE3' -- Onesub
            then true
            else false
        end as is_onesub
        , products.meals
        , products.portions
    from basket_products
    left join products
        on basket_products.company_id = products.company_id
        and basket_products.product_variation_id = products.product_variation_id
    where products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' --Mealbox
    and is_active_basket = true

)

select * from basket_products_filter_and_join_mealbox