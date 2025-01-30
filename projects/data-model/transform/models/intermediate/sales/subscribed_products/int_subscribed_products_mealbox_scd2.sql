{{ config(
  materialized="table"
) }}

with 

subscribed_products as (

    select * from {{ ref('int_subscribed_products_joined') }}
)

, signup_products as (

    select * from {{ ref('int_subscribed_products_at_signup') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, mealboxes_from_orders as (

    select * from {{ ref('int_subscribed_products_orders_without_deviations') }}

)

, preselector_devation_products as (

    select * from {{ ref('int_basket_deviations_preselector') }}

)

, financial_deviation_mealbox as (
    
    select * from {{ ref('int_basket_deviations_financial_product_mealbox_mapping')}}

)

, onesub_migration_mealbox as (
    
    select * from {{ ref('int_basket_deviations_onesub_migration')}}

)

, basket_mealbox as (

    select
        billing_agreement_id
        , billing_agreement_basket_id
        , company_id
        , product_id
        , meals
        , portions
        , product_variation_id
        , valid_from
        , basket_source
    from subscribed_products
    where product_type_id = '{{ var("mealbox_product_type_id") }}'

)

, preselector_mealbox as (

    select
          preselector_devation_products.billing_agreement_id
        , preselector_devation_products.billing_agreement_basket_id
        , preselector_devation_products.company_id
        , preselector_devation_products.product_id
        , preselector_devation_products.meals
        , preselector_devation_products.portions
        , preselector_devation_products.product_variation_id
        , preselector_devation_products.valid_from
    from preselector_devation_products 
    left join products
        on preselector_devation_products.product_variation_id = products.product_variation_id
        and preselector_devation_products.company_id = products.company_id
    where products.product_type_id = '{{ var("mealbox_product_type_id") }}'

)

, mealselector_mealbox as (
    select 
        billing_agreement_id
        , billing_agreement_basket_id
        , company_id
        , product_id
        , meals
        , portions
        , product_variation_id
        , valid_from
    from financial_deviation_mealbox
    where billing_agreement_basket_deviation_origin_id = '{{ var("mealselector_origin_id") }}' 
)

, signup_mealbox as (
    select
        signup_products.billing_agreement_id
        , signup_products.billing_agreement_basket_id
        , signup_products.company_id
        , products.product_id
        , products.meals
        , products.portions
        , signup_products.product_variation_id
        , signup_products.valid_from
    from signup_products
    left join products
        on signup_products.product_variation_id = products.product_variation_id
        and signup_products.company_id = products.company_id
    left join basket_mealbox
        on  signup_products.billing_agreement_basket_id = basket_mealbox.billing_agreement_basket_id
        and  signup_products.valid_from = basket_mealbox.valid_from
    where products.product_type_id = '{{ var("mealbox_product_type_id") }}'
    and basket_mealbox.product_variation_id is null --If we have rows coming from both signup and basket with same start date we discard the signup mealbox
)

-- Union of all sources we have on mealbox updates from customers
-- Information from normal deviations at a later stage, because we can not trust 
-- meals and portions to represent the basket in that case
, subscribed_mealboxes_unioned as (

    select 
    *
    from basket_mealbox

    union all

    select 
    * 
    , 'signup' as basket_source
    from signup_mealbox

    union all

    select 
    * 
    , 'preselector' as basket_source
    from preselector_mealbox

    union all

    select 
    * 
    , 'mealselector' as basket_source
    from mealselector_mealbox

    union all

    select 
    * 
    , 'orders' as basket_source
    from mealboxes_from_orders

    union all

    select 
    *
    , 'onesub_migration' as basket_source
    from onesub_migration_mealbox

)

-- we only track mealboxes after 01.01.2024 due to lacking data on history
, first_mealbox_after_history_start_date as (
    select 
        billing_agreement_basket_id
        , min(valid_from) as valid_from
    from subscribed_mealboxes_unioned 
    where valid_from > '{{ var("basket_history_start_at") }}'
    group by 1
)

-- Set first valid basket to the first logged basket and first valid from timestamp to 01-01-2024
-- for customers that was created before 01-01-2024
, subscribed_mealbox_add_first_row as (

    select
        subscribed_mealboxes_unioned.billing_agreement_basket_id
        , subscribed_mealboxes_unioned.company_id
        , subscribed_mealboxes_unioned.product_variation_id
        , subscribed_mealboxes_unioned.product_id
        , subscribed_mealboxes_unioned.meals
        , subscribed_mealboxes_unioned.portions
        , to_timestamp('{{ var("basket_history_start_at") }}') as valid_from
        , basket_source
    from subscribed_mealboxes_unioned
    left join billing_agreements
        on subscribed_mealboxes_unioned.billing_agreement_id = billing_agreements.billing_agreement_id
    left join first_mealbox_after_history_start_date
        on subscribed_mealboxes_unioned.billing_agreement_basket_id = first_mealbox_after_history_start_date.billing_agreement_basket_id
    where 
        billing_agreements.signup_at < '{{ var("basket_history_start_at") }}'
        and subscribed_mealboxes_unioned.valid_from = first_mealbox_after_history_start_date.valid_from

    union all

    select
        subscribed_mealboxes_unioned.billing_agreement_basket_id
        , subscribed_mealboxes_unioned.company_id
        , subscribed_mealboxes_unioned.product_variation_id
        , subscribed_mealboxes_unioned.product_id
        , subscribed_mealboxes_unioned.meals
        , subscribed_mealboxes_unioned.portions
        , valid_from
        , basket_source
    from subscribed_mealboxes_unioned

)

, subscribed_mealbox_add_valid_to as (
    select
    *
    , {{get_scd_valid_to('valid_from', 'billing_agreement_basket_id')}} as valid_to
    from subscribed_mealbox_add_first_row
)

-- Since the financial product for normal deviations represent the number of meals and portions that  
-- the customer ended up with after adjusting the order, we need a special logic for this. 
, customer_deviation_select_meals_and_portions as (
    select 
        financial_deviation_mealbox.billing_agreement_basket_id
        , financial_deviation_mealbox.company_id
        , financial_deviation_mealbox.product_id
        , case
            -- If the concept/product is equal to the basket we assume a diff in the meals is due to the customer 
            -- reducing or increasing the meals when doing the deviation. Otherwise, if the concept/product is different, 
            -- we trust the financial product over the basket, and hence choose meals from the financial product.
            when financial_deviation_mealbox.product_id = subscribed_mealbox_add_valid_to.product_id   
                then subscribed_mealbox_add_valid_to.meals
            else financial_deviation_mealbox.meals
        end as meals
        , case
            -- If the concept/product is equal to the basket we assume a diff in the portions is due to the customer 
            -- reducing or increasing the portions when doing the deviation. Otherwise, if the concept/product is different, 
            -- we trust the financial product over the basket, and hence choose portions from the financial product.
            when financial_deviation_mealbox.product_id = subscribed_mealbox_add_valid_to.product_id
                then subscribed_mealbox_add_valid_to.portions
            else financial_deviation_mealbox.portions
        end as portions
        , financial_deviation_mealbox.valid_from 
    from financial_deviation_mealbox
    left join subscribed_mealbox_add_valid_to
        on financial_deviation_mealbox.billing_agreement_basket_id = subscribed_mealbox_add_valid_to.billing_agreement_basket_id
        and financial_deviation_mealbox.valid_from >= subscribed_mealbox_add_valid_to.valid_from 
        and financial_deviation_mealbox.valid_from < subscribed_mealbox_add_valid_to.valid_to
    where 
        billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}' 
)

, customer_deviation_mealbox as (

    select 
        customer_deviation_select_meals_and_portions.billing_agreement_basket_id
        , products.product_variation_id
        , customer_deviation_select_meals_and_portions.valid_from
        , 'normal' as basket_source
    from customer_deviation_select_meals_and_portions
    left join products
        on  customer_deviation_select_meals_and_portions.product_id = products.product_id
        and customer_deviation_select_meals_and_portions.meals = products.meals
        and customer_deviation_select_meals_and_portions.portions = products.portions
        and customer_deviation_select_meals_and_portions.company_id = products.company_id
        and products.product_type_id = '{{ var("mealbox_product_type_id") }}'

)

, subscribed_mealbox_customer_deviation_mealbox_unioned as (
    select 
    billing_agreement_basket_id
    , product_variation_id
    , valid_from
    , basket_source
    from subscribed_mealbox_add_first_row

    union all
    
    select 
    billing_agreement_basket_id
    , product_variation_id
    , valid_from
    , basket_source
    from customer_deviation_mealbox
)


, subscribed_mealbox_recalculate_valid_to as (
    select
        billing_agreement_basket_id
        , product_variation_id
        , valid_from
        , {{get_scd_valid_to('valid_from', 'billing_agreement_basket_id')}} as valid_to
        , basket_source
    from subscribed_mealbox_customer_deviation_mealbox_unioned
)

-- group consecutive rows with same product variation id
, subscribed_mealbox_group_periods as (
    select 
        billing_agreement_basket_id
        , product_variation_id
        , valid_from
        , valid_to
        , basket_source
        , row_number() over 
            (
                partition by 
                    billing_agreement_basket_id 
                order by valid_from
            ) 
            - 
            row_number() over 
            (
                partition by 
                    billing_agreement_basket_id
                    , product_variation_id
                order by valid_from
            ) 
        as group
    from 
        subscribed_mealbox_recalculate_valid_to
)

-- merge consecutive rows with same product variation id
, subscribed_mealbox_merge_groups as (
    select
        billing_agreement_basket_id
        , product_variation_id
        , group
        , min(valid_from) as valid_from
        , max(valid_to) as valid_to
        , min_by(basket_source, valid_from) as basket_source_mealbox
    from 
        subscribed_mealbox_group_periods
    group by
        all
)

select * from subscribed_mealbox_merge_groups