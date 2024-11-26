with 

basket_products as (

    select * from {{ ref('int_basket_products_joined') }}
)

, signup_products as (

    select * from {{ ref('int_basket_products_at_signup') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, mealboxes_from_orders as (

    select * from {{ ref('int_basket_products_orders_without_deviations') }}

)

, preselector_devation_products as (

    select * from {{ ref('int_basket_deviations_preselector') }}

)

, financial_deviation_mealbox_products as (
    
    select * from {{ ref('int_basket_deviations_financial_product_mealbox_mapping')}}

)

, onesub_migration_deviation_mealbox_products as (
    
    select * from {{ ref('int_basket_deviations_onesub_migration')}}

)

, basket_mealbox as (

    select
        billing_agreement_basket_id
        , company_id
        , max(product_variation_id) as product_variation_id
        , product_id
        , meals
        , portions
        , valid_from
        , valid_to
        , signup_at
    from basket_products
    where product_type_id = '{{ var("mealbox_product_type_id") }}'
    group by all

)

, signup_mealbox as (
    select
          signup_products.billing_agreement_basket_id
        , signup_products.company_id
        , signup_products.product_variation_id
        , products.product_id
        , products.meals
        , products.portions
        , signup_products.valid_from
        , 'register' as source
    from signup_products
    left join products
        on signup_products.product_variation_id = products.product_variation_id
        and signup_products.company_id = products.company_id
    where products.product_type_id = '{{ var("mealbox_product_type_id") }}'
)

-- set valid from to beginning of 2024 for the first logged basket for each customer
-- where the customer was signed up before the history start date
-- and the first valid from is after the history start date
, basket_mealbox_change_first_valid_from as (
    select
        billing_agreement_basket_id
        , company_id
        , product_variation_id
        , product_id
        , meals
        , portions
        , case
            when
                signup_at < '{{ var("basket_history_start_at") }}'
                and valid_from > '{{ var("basket_history_start_at") }}'
                and row_number()
                    over (partition by billing_agreement_basket_id order by valid_from)
                = 1
                then to_timestamp('{{ var("basket_history_start_at") }}')
            else valid_from
        end
        as valid_from
        , 'basket' as source
    from basket_mealbox
)

, signup_and_basket_products_unioned as (
    select * from signup_mealbox
    union all
    select * from basket_mealbox_change_first_valid_from
)

, basket_mealbox_with_deviations_columns as (

    select
        billing_agreement_basket_id
        , company_id
        , product_id as basket_product_id
        , meals as basket_meals
        , portions as basket_portions
        , null as deviation_mealbox_product_id
        , null as deviation_mealbox_meals
        , null as deviation_mealbox_portions
        , valid_from
        , null as deviation_source
    from signup_and_basket_products_unioned

)

, preselector_deviation_mealbox_products as (

    select
        preselector_devation_products.* except(product_variation_quantity)
    from preselector_devation_products 
    left join products
        on preselector_devation_products.product_variation_id = products.product_variation_id
        and preselector_devation_products.company_id = products.company_id
    where products.product_type_id = '{{ var("mealbox_product_type_id") }}'

)

, deviation_mealbox_products_unioned as (

    select 
    *
    , 'orders' as deviation_source
    from mealboxes_from_orders

    union all

    select 
    * except(billing_agreement_basket_deviation_origin_id)
    , case
        when billing_agreement_basket_deviation_origin_id = '{{ var("mealselector_origin_id") }}' 
            then 'mealselector'
        when billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}'
            then 'financial'
        else billing_agreement_basket_deviation_origin_id
    end as deviation_source
    from financial_deviation_mealbox_products
    
    union
    
    select 
    *
    , 'onesub_migration' as deviation_source
    from onesub_migration_deviation_mealbox_products
    
    union
    
    select 
    *
    , 'preselector' as deviation_source
    from preselector_deviation_mealbox_products

)

, deviation_mealbox_with_basket_columns as (

    select 
    billing_agreement_basket_id
    , company_id
    , null as basket_product_id
    , null as basket_meals
    , null as basket_portions
    , product_id as deviation_mealbox_product_id
    , meals as deviation_mealbox_meals
    , portions as deviation_mealbox_portions
    , valid_from
    , deviation_source
    from deviation_mealbox_products_unioned

)

-- Union basket and deviation tables
, unioned_timeline as (
    select * from deviation_mealbox_with_basket_columns 
    union
    select * from basket_mealbox_with_deviations_columns
)

, add_recommendations_and_migrations_to_basket as (
    select 

    billing_agreement_basket_id
    , company_id
    , case 
        when deviation_source in ('preselector', 'mealselector', 'onesub_migration','orders')
            then deviation_mealbox_product_id    
        else basket_product_id
    end as basket_product_id
    , case 
        when deviation_source in ('preselector', 'mealselector', 'onesub_migration','orders')
            then deviation_mealbox_meals
        else basket_meals
    end as basket_meals
    , case 
        when deviation_source in ('preselector', 'mealselector', 'onesub_migration','orders')
            then deviation_mealbox_portions
        else basket_portions
    end as basket_portions
    , deviation_mealbox_product_id
    , deviation_mealbox_meals
    , deviation_mealbox_portions
    , valid_from
    , deviation_source

    from unioned_timeline
)

, fill_basket_values as (
    -- Fill in the missing basket values with the last non-null value.
    -- This is to ensure that we do not have gaps in the basket timeline 
    -- and that we have something to compare the deviation to
    select 
        billing_agreement_basket_id
        , company_id
        , last_value(basket_product_id, true) over (
            partition by billing_agreement_basket_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as basket_product_id
        , last_value(basket_meals, true) over (
            partition by billing_agreement_basket_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as basket_meals
        , last_value(basket_portions, true) over (
            partition by billing_agreement_basket_id
            order by valid_from
            rows between unbounded preceding and current row
        ) as basket_portions
        , deviation_mealbox_product_id
        , deviation_mealbox_meals
        , deviation_mealbox_portions
        , valid_from
        , deviation_source
    from add_recommendations_and_migrations_to_basket
)

, select_concept_meals_and_portions as (
    select
        billing_agreement_basket_id
        , company_id
        , coalesce(deviation_mealbox_product_id, basket_product_id) as product_id
        -- if customers has changed concept, then use meals from deviation
        , case
            when deviation_mealbox_product_id is not null and deviation_mealbox_product_id <> basket_product_id
                then deviation_mealbox_meals
            else coalesce(basket_meals, deviation_mealbox_meals)
        end as meals
        -- if customers has changed concept, then use portions from deviation
        , case
            when deviation_mealbox_product_id is not null and deviation_mealbox_product_id <> basket_product_id
                then deviation_mealbox_portions
            else coalesce(basket_portions, deviation_mealbox_portions)
        end as portions
        , valid_from
        , case
            when deviation_source in ('preselector', 'mealselector', 'onesub_migration','orders')
                then deviation_source
            when deviation_mealbox_product_id is not null and deviation_mealbox_product_id <> basket_product_id
                then 'financial full variation'
            when deviation_mealbox_product_id is not null
                then 'financial concept'
            else
                'subscription'
        end as source
    from fill_basket_values
)


, select_product_variation as 
(
    select 
        select_concept_meals_and_portions.billing_agreement_basket_id
        , products.product_variation_id
        , select_concept_meals_and_portions.valid_from
        , select_concept_meals_and_portions.source
    from select_concept_meals_and_portions
    left join products
        on select_concept_meals_and_portions.product_id = products.product_id
        and select_concept_meals_and_portions.meals = products.meals
        and select_concept_meals_and_portions.portions = products.portions
        and select_concept_meals_and_portions.company_id = products.company_id
        and products.product_type_id = '{{ var("mealbox_product_type_id") }}'
)


, add_valid_to as (
    select
        billing_agreement_basket_id
        , product_variation_id
        , valid_from
        , {{get_scd_valid_to('valid_from', 'billing_agreement_basket_id')}} as valid_to
        , source
    from select_product_variation
)

-- group consecutive rows with same product variation id
, group_periods as (
    select 
        billing_agreement_basket_id
        , product_variation_id
        , valid_from
        , valid_to
        , source
        , row_number() over (partition by billing_agreement_basket_id order by valid_from) - 
            row_number() over (partition by billing_agreement_basket_id, product_variation_id order by valid_from) as group
    from 
        add_valid_to
)

-- merge consecutive rows with same product variation id
, merge_groups as (
    select
        billing_agreement_basket_id
        , product_variation_id
        , group
        , min(valid_from) as valid_from
        , max(valid_to) as valid_to
    from 
        group_periods
    group by
        all
)

select * from merge_groups

