with 

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, first_orders as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

)

, status_names as (

    select * from {{ ref('cms__billing_agreement_statuses') }}

)

, preferences_scd2 as (

    select
        billing_agreement_preferences_updated_id
        , billing_agreement_id
        , valid_from
        , valid_to
    from {{ref('int_billing_agreement_preferences_unioned')}}

)

, basket_products_scd2 as (

    select * from {{ ref('int_billing_agreements_basket_mealbox_scd') }}

)

/*, scd_loyalty_level as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd') }}

) */

, billing_agreements_scd1 as (
    select * from billing_agreements
    where valid_to is null
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

-- Use macro to join all scd2 tables
{% set id_column = 'billing_agreement_id' %}
{% set table_names = [
    'base_scd2' 
    , 'billing_agreements_scd2'
    , 'basket_products_scd2'
    , 'preferences_scd2'
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
        , billing_agreements_scd1.company_id
        , billing_agreements_scd1.payment_method
        , billing_agreements_scd1.signup_source
        , billing_agreements_scd1.signup_salesperson
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
        , scd2_tables_joined.*
    
    from scd2_tables_joined
    left join billing_agreements_scd1
        on scd2_tables_joined.billing_agreement_id = billing_agreements_scd1.billing_agreement_id
    left join first_orders
        on scd2_tables_joined.billing_agreement_id = first_orders.billing_agreement_id

)

select * from all_tables_joined