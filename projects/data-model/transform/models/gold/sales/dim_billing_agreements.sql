with 

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, onesub_beta_agreements as (

    select * from {{ ref('cms__onesub_beta_agreements') }}

)

, first_orders as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

)

, billing_agreement_statuses as (

    select * from {{ ref('int_billing_agreements_statuses_scd2') }}
    
)

, preferences as (

    select * from {{ref('int_billing_agreement_preferences_unioned')}}

)

, subscribed_products as (

    select *  from {{ ref('int_subscribed_products_scd2') }}

)

, products as (

    select *  from {{ ref('int_product_tables_joined') }}

)

, loyalty_levels as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd2') }}

)

, reactivation_cohorts as (

    select * from {{ ref('int_reactivation_customer_cohorts') }}

)

, billing_agreements_partnerships as (

    select * from {{ ref('int_billing_agreements_partnerships_rules_aggregated') }}

)

, agreements_to_include as (
    
    select distinct billing_agreement_id from billing_agreements

)

, onesub_agreements as (
    select 
    subscribed_products.billing_agreement_basket_product_updated_id
    , max(product_id) as product_id_mealbox
    from subscribed_products
    left join products
        on subscribed_products.company_id = products.company_id
        and subscribed_products.product_variation_id = products.product_variation_id
    where products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' --Mealbox
    group by 1
    having max(product_id) = 'D699150E-D2DE-4BC1-A75C-8B70C9B28AE3' -- Onesub
)

, billing_agreements_scd1 as (
    select * from billing_agreements
    where valid_to = '{{ var("future_proof_date") }}'
)

, base_scd2 as (
    select 
        billing_agreements_scd1.billing_agreement_id
        , case 
            when first_orders.first_order_created_at < billing_agreements_scd1.signup_at
            then first_orders.first_order_created_at
            else billing_agreements_scd1.signup_at 
            end as valid_from
        , {{ get_scd_valid_to() }} as valid_to

    from billing_agreements_scd1
    left join first_orders
        on billing_agreements_scd1.billing_agreement_id = first_orders.billing_agreement_id

)

, subscribed_products_scd2 as (

    select distinct
    subscribed_products.billing_agreement_basket_product_updated_id
    , subscribed_products.billing_agreement_id
    , subscribed_products.has_grocery_subscription
    , subscribed_products.valid_from
    , subscribed_products.valid_to
    from subscribed_products
    where subscribed_products.billing_agreement_id is not null

)

, preferences_scd2 as (

    select
        billing_agreement_preferences_updated_id
        , preference_combination_id
        , billing_agreement_id
        , valid_from
        , valid_to
    from preferences

)

, loyalty_levels_scd2 as (

    select
        billing_agreement_id
        , loyalty_level_name_brand
        , loyalty_level_name_english
        , loyalty_level_number
        , valid_from
        , valid_to
    from loyalty_levels

)

, reactivation_cohorts_scd2 as (

    select
        billing_agreement_id
        , menu_week_cutoff_at_from as valid_from
        , menu_week_cutoff_at_to as valid_to
        , menu_week_monday_date_from as reactivation_monday_date
        , {{ get_financial_date_from_monday_date('menu_week_monday_date_from') }} as reactivation_financial_date
    from reactivation_cohorts
)

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_id' %}
{% set table_names = [
    'base_scd2' 
    , 'billing_agreement_statuses'
    , 'subscribed_products_scd2'
    , 'preferences_scd2'
    , 'loyalty_levels_scd2'
    , 'reactivation_cohorts_scd2'
    ] %}

, scd2_tables_joined as (
    
    {{ join_scd2_tables(id_column, table_names) }}

)

, all_tables_joined as (

    select
        md5(
            concat(
            cast(scd2_tables_joined.billing_agreement_id as string),
            cast(scd2_tables_joined.valid_from as string)
            )
        ) as pk_dim_billing_agreements
        , scd2_tables_joined.valid_from
        , scd2_tables_joined.valid_to
        , scd2_tables_joined.is_current
        , scd2_tables_joined.billing_agreement_id
        , scd2_tables_joined.billing_agreement_preferences_updated_id
        , scd2_tables_joined.preference_combination_id
        , scd2_tables_joined.billing_agreement_basket_product_updated_id
        , scd2_tables_joined.loyalty_level_name_brand
        , scd2_tables_joined.loyalty_level_name_english
        , scd2_tables_joined.loyalty_level_number
        , billing_agreements_scd1.company_id
        , billing_agreements_scd1.payment_method
        , billing_agreements_scd1.signup_source
        , billing_agreements_scd1.signup_salesperson
        , billing_agreements_scd1.signup_at
        , billing_agreements_scd1.signup_date
        , billing_agreements_scd1.signup_year_day
        , billing_agreements_scd1.signup_month_day
        , billing_agreements_scd1.signup_week_day
        , billing_agreements_scd1.signup_week
        , billing_agreements_scd1.signup_month
        , billing_agreements_scd1.signup_month_number
        , billing_agreements_scd1.signup_quarter
        , billing_agreements_scd1.signup_year
        , first_orders.first_menu_week_monday_date
        , first_orders.first_menu_week_week
        , first_orders.first_menu_week_month
        , first_orders.first_menu_week_month_number
        , first_orders.first_menu_week_quarter
        , first_orders.first_menu_week_year
        , scd2_tables_joined.reactivation_financial_date as reactivation_date
        , extract('WEEK', scd2_tables_joined.reactivation_financial_date) as reactivation_week
        , date_format(scd2_tables_joined.reactivation_financial_date, 'MMMM') as reactivation_month
        , extract('MONTH', scd2_tables_joined.reactivation_financial_date) as reactivation_month_number
        , extract('QUARTER', scd2_tables_joined.reactivation_financial_date) as reactivation_quarter
        , extract('YEAROFWEEK', scd2_tables_joined.reactivation_financial_date) as reactivation_year
        , scd2_tables_joined.billing_agreement_status_name
        , scd2_tables_joined.has_grocery_subscription
        , case
            when onesub_agreements.billing_agreement_basket_product_updated_id is null
            then 'Not OneSub'
            else 'OneSub'
        end as onesub_flag
        , case 
            when onesub_beta_agreements.is_internal is false then "10% Customer Launch"
            when onesub_beta_agreements.is_internal is true then "Internal Launch"
        else "Normal Customer"
        end as onesub_beta_flag
        , billing_agreements_partnerships.partnership_name
    from scd2_tables_joined
    left join billing_agreements_scd1
        on scd2_tables_joined.billing_agreement_id = billing_agreements_scd1.billing_agreement_id
    left join first_orders
        on scd2_tables_joined.billing_agreement_id = first_orders.billing_agreement_id
    left join onesub_agreements
        on scd2_tables_joined.billing_agreement_basket_product_updated_id = onesub_agreements.billing_agreement_basket_product_updated_id
    left join onesub_beta_agreements
        on scd2_tables_joined.billing_agreement_id = onesub_beta_agreements.billing_agreement_id
    
    -- when customers signup during the ingestion in the ETL pipeline, billing agreements that does not yet exist in 
    -- cms__billing_agreements, might exist in other tables. We want to exclude all billing agreements from 
    -- dim_billling_agreements that does not exist in cms__billing_agreements yet.
    inner join agreements_to_include
        on scd2_tables_joined.billing_agreement_id = agreements_to_include.billing_agreement_id
    left join billing_agreements_partnerships
        on scd2_tables_joined.billing_agreement_id = billing_agreements_partnerships.billing_agreement_id

)

select * from all_tables_joined
