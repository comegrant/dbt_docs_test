with

products as (
    select * from {{ ref('int_product_tables_joined') }}
)

, extract_distinct_meals as (
    select 
    distinct coalesce(meals,0) as meals 
    from products
)

, add_pk as (
    select
        meals as pk_dim_meals
        , meals
    from extract_distinct_meals
)

select * from add_pk
