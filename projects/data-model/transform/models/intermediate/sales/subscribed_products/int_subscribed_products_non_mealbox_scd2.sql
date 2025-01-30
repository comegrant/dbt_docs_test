{{ config(
  materialized="table"
) }}

with 

subscribed_products as (

    select * from {{ ref('int_subscribed_products_joined') }}
)

, preselector_devation_products as (

    select * from {{ ref('int_basket_deviations_preselector') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, basket_non_mealbox_products as (

    select
        billing_agreement_basket_id
        , product_variation_id
        , product_variation_quantity
        , is_extra_product
        , valid_from
        , basket_source
    from subscribed_products
    where product_type_id <> '{{ var("mealbox_product_type_id") }}'
    group by all

)

, preselector_deviation_non_mealbox_products as (

    select
        preselector_devation_products.billing_agreement_basket_id
        , preselector_devation_products.product_variation_id
        , preselector_devation_products.product_variation_quantity
        , false as is_extra_product
        , preselector_devation_products.valid_from
        , 'preselector' as basket_source
    from preselector_devation_products
    left join products
        on 
            preselector_devation_products.product_variation_id = products.product_variation_id
            and preselector_devation_products.company_id = products.company_id
        where products.product_type_id <> '{{ var("mealbox_product_type_id") }}' -- Mealbox
        and products.product_type_id <> '{{ var("velg&vrak_product_type_id") }}' -- velg&vrak

)

, basket_and_preselector_unioned as (

    select * from preselector_deviation_non_mealbox_products

    union

    select * from basket_non_mealbox_products

)

, create_basket_products_list as (

  select 
    billing_agreement_basket_id
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
        , basket_source
  from basket_and_preselector_unioned
  group by billing_agreement_basket_id, valid_from, basket_source

)

, add_valid_to as (

  select 
    create_basket_products_list.*
    , {{get_scd_valid_to('valid_from', 'billing_agreement_basket_id')}} as valid_to
  from create_basket_products_list

)

-- group consecutive rows with same product variation id
, group_periods as (
    select 
        billing_agreement_basket_id
        , basket_products_list
        , valid_from
        , valid_to
        , basket_source
        , row_number() over (partition by billing_agreement_basket_id order by valid_from) - 
            row_number() over (partition by billing_agreement_basket_id, basket_products_list order by valid_from) as group
    from 
        add_valid_to
)

-- merge consecutive rows with same product variation id
, merge_groups as (
    select
        billing_agreement_basket_id
        , basket_products_list
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
