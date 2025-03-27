{{ config(
  materialized="table"
) }}

with 

basket_products as (

    select * from {{ ref('int_basket_products_joined') }}
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

    select * from {{ ref('int_ordered_mealboxes_without_deviations') }}

)

, preselector_deviation_products as (

    select * from {{ ref('int_basket_deviations_preselector') }}

)

, financial_deviation_mealbox as (
    
    select * from {{ ref('int_basket_deviations_financial_product_mealbox_mapping')}}

)

, mealboxes_from_onesub_migration as (
    
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
    from basket_products
    where product_type_id = '{{ var("mealbox_product_type_id") }}'

)

-- find first valid from timestamp from dbt_snapshot
, dbt_snapshot_min_valid_from as (
    select
        billing_agreement_basket_id
        , min(valid_from) as min_valid_from
    from basket_mealbox
    where basket_source = 'dbt_snapshots'
    group by 1
)

, preselector_mealbox as (

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
    from preselector_deviation_products 
    where product_type_id = '{{ var("mealbox_product_type_id") }}'

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
        , 'mealselector deviation' as basket_source
    from financial_deviation_mealbox
    where billing_agreement_basket_deviation_origin_id = '{{ var("mealselector_origin_id") }}' 
)

, signup_mealbox as (
    select
        signup_products.billing_agreement_id
        , signup_products.billing_agreement_basket_id
        , signup_products.company_id
        , signup_products.product_id
        , signup_products.meals
        , signup_products.portions
        , signup_products.product_variation_id
        , signup_products.valid_from
        , signup_products.basket_source
    from signup_products
    left join dbt_snapshot_min_valid_from
        on signup_products.billing_agreement_basket_id = dbt_snapshot_min_valid_from.billing_agreement_basket_id
    -- only include signup mealbox if history is missing in the dbt snapshot of the basket
    where (
        signup_products.valid_from < dbt_snapshot_min_valid_from.min_valid_from 
        or dbt_snapshot_min_valid_from.billing_agreement_basket_id is null)
    and signup_products.product_type_id = '{{ var("mealbox_product_type_id") }}'
    
)

, orders_mealbox as (
    select
        mealboxes_from_orders.billing_agreement_id
        , mealboxes_from_orders.billing_agreement_basket_id
        , mealboxes_from_orders.company_id
        , mealboxes_from_orders.product_id
        , mealboxes_from_orders.meals
        , mealboxes_from_orders.portions
        , mealboxes_from_orders.product_variation_id
        , mealboxes_from_orders.valid_from
        , mealboxes_from_orders.basket_source
    from mealboxes_from_orders
    left join dbt_snapshot_min_valid_from
        on mealboxes_from_orders.billing_agreement_basket_id = dbt_snapshot_min_valid_from.billing_agreement_basket_id
    -- only include signup mealbox if history is missing in the dbt snapshot of the basket
    where (
        mealboxes_from_orders.valid_from < dbt_snapshot_min_valid_from.min_valid_from
        or dbt_snapshot_min_valid_from.billing_agreement_basket_id is null
    )

)

, onesub_migration_mealbox as (

    select
        mealboxes_from_onesub_migration.billing_agreement_id
        , mealboxes_from_onesub_migration.billing_agreement_basket_id
        , mealboxes_from_onesub_migration.company_id
        , mealboxes_from_onesub_migration.product_id
        , mealboxes_from_onesub_migration.meals
        , mealboxes_from_onesub_migration.portions
        , mealboxes_from_onesub_migration.product_variation_id
        , mealboxes_from_onesub_migration.valid_from
        , mealboxes_from_onesub_migration.basket_source
    from mealboxes_from_onesub_migration

)

, customer_deviation_mealbox as (
    
    select
        financial_deviation_mealbox.billing_agreement_id
        , financial_deviation_mealbox.billing_agreement_basket_id
        , financial_deviation_mealbox.company_id
        , financial_deviation_mealbox.product_id
        , financial_deviation_mealbox.meals
        , financial_deviation_mealbox.portions
        , financial_deviation_mealbox.product_variation_id
        , financial_deviation_mealbox.valid_from
        , 'customer deviation' as basket_source
    from financial_deviation_mealbox
    left join dbt_snapshot_min_valid_from
        on financial_deviation_mealbox.billing_agreement_basket_id = dbt_snapshot_min_valid_from.billing_agreement_basket_id
    -- only include customer deviation mealbox if history is missing in the dbt snapshot of the basket
    where (
        financial_deviation_mealbox.valid_from < dbt_snapshot_min_valid_from.min_valid_from
        or dbt_snapshot_min_valid_from.billing_agreement_basket_id is null
    )
    and billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}' 

)

-- Union of all sources we have on mealbox updates from customers
-- Information from normal deviations at a later stage, because we can not trust 
-- meals and portions to represent the basket in that case
, subscribed_mealboxes_unioned as (

    -- Main mealbox subscription source: 
    -- Tracks the subscription of customers to capture changes.
    -- Based on snapshots taken every week from April 2024 and every second hour from October 2024.
    select 
    *
    from basket_mealbox

    union all

    -- Complementing mealbox subscription source: 
    -- Trusthworthy source of subscribed mealbox, catches changes that can have happened inbetween snapshots.
    -- Based on deviations which is not made by the customer.
    select 
    *
    from preselector_mealbox

    union all

    select 
    * 
    from mealselector_mealbox

    union all

    select 
    *
    from onesub_migration_mealbox

    union all

    -- Supplementing mealbox subscription source: 
    -- Used for customers that are missing data on historically subscribed products.
    -- Before October 2024 we are missing history for some customers.
    select 
    * 
    from orders_mealbox

    union all

    select 
    * 
    from signup_mealbox

    union all

    select 
    * 
    from customer_deviation_mealbox

)

, subscribed_mealbox_add_valid_to as (
    select
    *
    , {{get_scd_valid_to('valid_from', 'billing_agreement_basket_id')}} as valid_to
    from subscribed_mealboxes_unioned
)

-- (Pre-Onesub):
-- The customer deviation mealbox represents the product variation the customer
-- ended up with after adjusting the order.
-- If the customer deviation has the same product as the previous row which was 
-- not a customer deviation, we want to fetch the product variation from the previous row.
-- If the customer deviation has a different product as the previous row which was 
-- not a customer deviation, we interpret it as a customer changeing the subscription and hence 
-- keep the product variation from the customer deviation.

-- Fill down product id and product variation id for the product on the row above the customer deviations
, find_previous_product_variation as (

  select
    *
    -- Get the most recent product_variation_id from a non-customer deviation row
    , last_value(
        case 
            when basket_source <> 'customer deviation' then product_id 
        end, true
        ) over (
        partition by billing_agreement_id
        order by valid_from 
        rows between unbounded preceding and current row
    ) as previous_product_id
    , last_value(
        case 
            when basket_source <> 'customer deviation' then product_variation_id 
        end, true
        ) over (
        partition by billing_agreement_id
        order by valid_from 
        rows between unbounded preceding and current row
    ) as previous_product_variation_id
  from subscribed_mealbox_add_valid_to

)

-- Replace product variation id for customer deviations
-- if product id is the same as for the previous
-- row which was not a customer deviation
, replace_product_variation as (

    select 
        billing_agreement_id
        , billing_agreement_basket_id
        , case 
            when basket_source = 'customer deviation' 
            and product_id = previous_product_id 
            then previous_product_variation_id
            else product_variation_id
        end as product_variation_id
        , valid_from
        , valid_to
        , basket_source
        
    from find_previous_product_variation

)

-- group consecutive rows with same product variation id
, subscribed_mealbox_group_periods as (
    select 
        billing_agreement_id
        , billing_agreement_basket_id
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
        as group_periods
    from 
        replace_product_variation
)

-- merge consecutive rows with same product variation id
, subscribed_mealbox_merge_groups as (
    select
        billing_agreement_id
        , billing_agreement_basket_id
        , product_variation_id
        , group_periods
        , min(valid_from) as valid_from
        , max(valid_to) as valid_to
        , min_by(basket_source, valid_from) as basket_source_mealbox
    from 
        subscribed_mealbox_group_periods
    group by
        all
)



-- for all agreements that signed up before the history start date
-- and that does not have a dbt_snapshot before the history start date
-- we want to get the first subscribed mealbox they had after the history start date
, find_first_row_after_history_start as (
    
    select
        subscribed_mealbox_group_periods.billing_agreement_id
        , min(subscribed_mealbox_group_periods.group_periods) as group_periods
    from subscribed_mealbox_group_periods
    left join dbt_snapshot_min_valid_from
        on subscribed_mealbox_group_periods.billing_agreement_basket_id = dbt_snapshot_min_valid_from.billing_agreement_basket_id
    left join billing_agreements
        on subscribed_mealbox_group_periods.billing_agreement_id = billing_agreements.billing_agreement_id
    where (dbt_snapshot_min_valid_from.min_valid_from > '{{ var("basket_history_start_at") }}' or dbt_snapshot_min_valid_from.min_valid_from is null)
        and billing_agreements.signup_at < '{{ var("basket_history_start_at") }}'
        and subscribed_mealbox_group_periods.valid_from > '{{ var("basket_history_start_at") }}'
    group by 1

)

-- for all agreements that signed up before the history start date
-- and that does not have a dbt_snapshot before the history start date
-- we trust the first subscried product after the history start date
-- over the last subscribed product before the history start date
, adjust_first_subscribed_product as (

    select 
        subscribed_mealbox_merge_groups.billing_agreement_basket_id
        , subscribed_mealbox_merge_groups.product_variation_id
        , case 
            when subscribed_mealbox_merge_groups.group_periods = find_first_row_after_history_start.group_periods
            then to_timestamp('{{ var("basket_history_start_at") }}')
            else subscribed_mealbox_merge_groups.valid_from
        end as valid_from
        , subscribed_mealbox_merge_groups.valid_to
        , subscribed_mealbox_merge_groups.basket_source_mealbox
    from subscribed_mealbox_merge_groups
    left join find_first_row_after_history_start
        on subscribed_mealbox_merge_groups.billing_agreement_id = find_first_row_after_history_start.billing_agreement_id
    where (
        subscribed_mealbox_merge_groups.group_periods >= find_first_row_after_history_start.group_periods
        and subscribed_mealbox_merge_groups.billing_agreement_id = find_first_row_after_history_start.billing_agreement_id
    )
    or find_first_row_after_history_start.billing_agreement_id is null

)

select * from adjust_first_subscribed_product