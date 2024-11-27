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

, status_names as (

    select * from {{ ref('cms__billing_agreement_statuses') }}

)

, preferences as (

    select * from {{ref('int_billing_agreement_preferences_unioned')}}

)

, basket_products_joined as (

    select *  from {{ ref('int_basket_products_scd2') }}

)

, products as (

    select *  from {{ ref('int_product_tables_joined') }}

)

, recommendations as (

    select * from {{ ref('int_basket_deviation_recommendations') }}

)

/*, scd_loyalty_level as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd2') }}

) */

, onesub_agreements as (
    select 
    basket_products_joined.billing_agreement_basket_product_updated_id
    , max(product_id) as product_id_mealbox
    from basket_products_joined
    left join products
        on basket_products_joined.company_id = products.company_id
        and basket_products_joined.product_variation_id = products.product_variation_id
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
            when first_orders.source_created_at < billing_agreements_scd1.signup_at
            then first_orders.source_created_at
            else billing_agreements_scd1.signup_at 
            end as valid_from
        , {{ get_scd_valid_to() }} as valid_to

    from billing_agreements_scd1
    left join first_orders
        on billing_agreements_scd1.billing_agreement_id = first_orders.billing_agreement_id

)

, preselector_agreements_scd2 as (

    select 
        billing_agreement_id
        , "Preselector" as preselector_flag
        , min(deviation_created_at) as valid_from
        , {{ get_scd_valid_to() }} as valid_to
    from recommendations
    where billing_agreement_basket_deviation_origin_id = '{{ var("preselector_origin_id") }}'
    group by 1,2,4

)

, billing_agreements_scd2 as (

    select
        billing_agreements.billing_agreement_id
        , status_names.billing_agreement_status_name
        , billing_agreements.sales_point_id
        , billing_agreements.valid_from
        , {{ get_scd_valid_to('billing_agreements.valid_to') }} as valid_to
    from billing_agreements
    left join status_names
        on billing_agreements.billing_agreement_status_id = status_names.billing_agreement_status_id
)

, basket_products_scd2 as (

    select distinct
    basket_products_joined.billing_agreement_basket_product_updated_id
    , basket_products_joined.billing_agreement_id
    , basket_products_joined.valid_from
    , basket_products_joined.valid_to
    from basket_products_joined
    where basket_products_joined.billing_agreement_id is not null

)

, preferences_scd2 as (

    select
        billing_agreement_preferences_updated_id
        , billing_agreement_id
        , valid_from
        , valid_to
    from preferences

)

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_id' %}
{% set table_names = [
    'base_scd2' 
    , 'billing_agreements_scd2'
    , 'basket_products_scd2'
    , 'preferences_scd2'
    , 'preselector_agreements_scd2'
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
        , scd2_tables_joined.billing_agreement_basket_product_updated_id
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
        , billing_agreements_scd1.signup_quarter
        , billing_agreements_scd1.signup_year
        , first_orders.first_menu_week_monday_date
        , first_orders.first_menu_week_week
        , first_orders.first_menu_week_month
        , first_orders.first_menu_week_quarter
        , first_orders.first_menu_week_year
        , scd2_tables_joined.billing_agreement_status_name
        , scd2_tables_joined.sales_point_id
        , coalesce(scd2_tables_joined.preselector_flag, "Not Preselector") as preselector_flag
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
    from scd2_tables_joined
    left join billing_agreements_scd1
        on scd2_tables_joined.billing_agreement_id = billing_agreements_scd1.billing_agreement_id
    left join first_orders
        on scd2_tables_joined.billing_agreement_id = first_orders.billing_agreement_id
    left join onesub_agreements
        on scd2_tables_joined.billing_agreement_basket_product_updated_id = onesub_agreements.billing_agreement_basket_product_updated_id
    left join onesub_beta_agreements
        on scd2_tables_joined.billing_agreement_id = onesub_beta_agreements.billing_agreement_id

)

select * from all_tables_joined
