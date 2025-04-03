{{ config(
  materialized="table"
) }}


with 

basket_mealboxes as (

    select * from {{ ref('int_subscribed_products_mealbox_scd2') }}

)

, basket_non_mealboxes as (

    select * from {{ ref('int_subscribed_products_non_mealbox_scd2') }}

)

, baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, baskets_scd1 as (
    
    select distinct
        baskets.billing_agreement_basket_id
        , baskets.billing_agreement_id
        , billing_agreements.company_id 
    from baskets
    left join billing_agreements
    on 
        baskets.billing_agreement_id = billing_agreements.billing_agreement_id
        and billing_agreements.valid_to = '{{ var("future_proof_date") }}'
    where baskets.valid_to = '{{ var("future_proof_date") }}'
    and billing_agreements.billing_agreement_id is not null

)

, baskets_scd2 as (
    select

        billing_agreement_basket_id
        , shipping_address_id
        , basket_delivery_week_type_id
        , timeblock_id
        , is_default_basket
        , is_active_basket
        , valid_from
        , valid_to

    from baskets
)

, mealboxes_scd2 as (

    select

        billing_agreement_basket_id
        , product_variation_id as mealbox_product_variation_id
        , valid_from
        , valid_to
        , basket_source_mealbox

    from basket_mealboxes

)

, non_mealboxes_scd2 as (

    select
         billing_agreement_basket_id
        , basket_products_list
        , has_grocery_subscription
        , valid_from
        , valid_to
        , basket_source_groceries

    from basket_non_mealboxes

)

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_basket_id' %}
{% set table_names = [
    'baskets_scd2', 
    'mealboxes_scd2',
    'non_mealboxes_scd2'
    ] %}

, scd2_tables_joined as (
    
    {{ join_scd2_tables(id_column, table_names) }}

)

, add_scd1 as (

    select
        baskets_scd1.company_id
        , baskets_scd1.billing_agreement_id
        , scd2_tables_joined.*
    from scd2_tables_joined
    left join baskets_scd1
        on scd2_tables_joined.billing_agreement_basket_id = baskets_scd1.billing_agreement_basket_id
)

, add_mealbox_to_product_list as (
    
    select 
    add_scd1.* except(basket_products_list)
    , case
        when basket_products_list is null 
            then 
                array(
                    struct(
                        mealbox_product_variation_id as product_variation_id
                        , 1 as product_variation_quantity
                        , false as is_extra_product
                    )
                )
        else
            array_union(
                basket_products_list, 
                array(
                    struct(
                        mealbox_product_variation_id as product_variation_id
                        , 1 as product_variation_quantity
                        , false as is_extra_product
                    )
                )
            ) 
    end as basket_products_list
    from add_scd1 

)

, remove_nulls_for_product_variation_id as (
-- Since a basket can exist without any mealbox product, there will be periods on the 
-- mealbox timeline where the product_variation_id is null. 
-- These null values can be removed after we did the merge with non-mealbox products
    select 
    * except(basket_products_list)
    , filter(basket_products_list,x -> x.product_variation_id is not null) as basket_products_list
    from add_mealbox_to_product_list

)

, explode_products as (

    select 
        md5(concat(
                cast(billing_agreement_basket_id as string)
                , cast(valid_from as string)
            )) as billing_agreement_basket_product_updated_id
        , billing_agreement_basket_id
        , company_id
        , billing_agreement_id
        , shipping_address_id
        , basket_delivery_week_type_id
        , timeblock_id
        , is_default_basket
        , coalesce(is_active_basket, false) as is_active_basket
        , basket_product_object.product_variation_id
        , basket_product_object.product_variation_quantity
        , basket_product_object.is_extra_product
        , coalesce(has_grocery_subscription, false) as has_grocery_subscription
        , valid_from
        , valid_to
        , basket_source_groceries
        , basket_source_mealbox
    from remove_nulls_for_product_variation_id
    lateral view outer explode(basket_products_list) as basket_product_object

)

, filter_basket_history as (
    -- All rows that has valid_to before the history start should not be included,
    -- and all rows that are then left (which should be a maximum of 1 per basket)
    -- should have their valid_from set to the history start date.
    -- This is to ensure that we only keep the rows that are within the selectedhistory window
    select 
    * except(valid_from)
    , case
        when valid_from < '{{ var("basket_history_start_at") }}'
            then to_timestamp('{{ var("basket_history_start_at") }}') --Set valid_from to the history start date for all rows that has valid_from before the history start date
        else valid_from
    end as valid_from
    from explode_products
    where valid_to > '{{ var("basket_history_start_at") }}' --Filter away all rows that has valid_to before the history start date
)

select * from filter_basket_history

