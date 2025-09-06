with

products as (
    select * from {{ ref('int_product_tables_joined') }}
)

, extract_distinct_meals as (

    select 
    distinct 
        coalesce(products.meals, 0) as meals
        , coalesce(mealbox_products.meals, 0) as mealbox_meals
    from products
    cross join products as mealbox_products
        on mealbox_products.product_type_id in ('{{ var("mealbox_product_type_id") }}', '{{ var("financial_product_type_id") }}')

    union

    select 
    distinct 
        coalesce(products.meals, 0) as meals
        , 0 as mealbox_meals
    from products

)

, add_pk as (

    select
        concat_ws('-', cast(meals as string), cast(mealbox_meals as string)) as pk_dim_meals
        , meals
        , mealbox_meals
    from extract_distinct_meals

)

select * from add_pk