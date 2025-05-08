{{ config(
  materialized="table"
) }}

with 

subscribed_products as (

    select * from {{ ref('int_basket_products_joined') }}
)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, basket_non_mealbox_products as (

    select
        billing_agreement_basket_id
        , billing_agreement_id
        , product_variation_id
        , product_variation_quantity
        , is_extra_product
        , valid_from
        , valid_to
        , basket_source
        , case when product_type_id in ({{ var('grocery_product_type_ids') | join(', ') }}) then true else false end as is_grocery
    from subscribed_products
    where product_type_id <> '{{ var("mealbox_product_type_id") }}'
    group by all

)

, create_basket_products_list as (

  select 
    billing_agreement_basket_id
    , billing_agreement_id
    , array_sort(
        collect_list(
            struct(
            product_variation_id 
            , product_variation_quantity
            , is_extra_product
            )
        )
    ) as basket_products_list
        , valid_from
        , valid_to
        , basket_source
        , max(is_grocery) as has_grocery_subscription
  from basket_non_mealbox_products
  group by all

)

-- we don't want to merge rows with gaps in validity period, 
-- so we add a is_new_island flag to identify the start of a new island
, identify_scd2_islands as (
    
    select 
        billing_agreement_basket_id
        , billing_agreement_id
        , basket_products_list
        , has_grocery_subscription
        , valid_from
        , valid_to
        , basket_source
        , case 
            when lag(valid_to) over (partition by billing_agreement_basket_id order by valid_from) = valid_to then 0
            else 1
        end as is_new_island
    from create_basket_products_list
)

-- group consecutive rows within the same island and with same product variation id
, group_periods as (
    select 
        billing_agreement_basket_id
        , billing_agreement_id
        , basket_products_list
        , has_grocery_subscription
        , valid_from
        , valid_to
        , basket_source
        , sum(is_new_island) over (partition by billing_agreement_basket_id, basket_products_list order by valid_from rows unbounded preceding) as group
    from 
        identify_scd2_islands
)

-- merge consecutive rows with same product variation id
, merge_groups as (
    select
        billing_agreement_basket_id
        , billing_agreement_id
        , basket_products_list
        , has_grocery_subscription
        , group
        , min(valid_from) as valid_from
        , max(valid_to) as valid_to
        , min_by(basket_source, valid_from) as basket_source_groceries
    from 
        group_periods
    group by
        all
)

select * from merge_groups
