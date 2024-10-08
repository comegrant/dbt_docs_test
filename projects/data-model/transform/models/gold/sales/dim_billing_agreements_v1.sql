with 

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, first_orders as (

    select * 
    from {{ ref('int_billing_agreements_extract_first_order') }}

)

, status_names as (

    select * from {{ ref('cms__billing_agreement_statuses') }}

)

/*, scd_loyalty_level as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd') }}

) */

, fixed_columns as (

    select 

    billing_agreements.billing_agreement_id
    , billing_agreements.company_id

    , billing_agreements.payment_method
    , billing_agreements.signup_source
    , billing_agreements.signup_salesperson

    , billing_agreements.signup_date
    , billing_agreements.signup_year_day
    , billing_agreements.signup_month_day
    , billing_agreements.signup_week_day
    , billing_agreements.signup_week
    , billing_agreements.signup_month
    , billing_agreements.signup_quarter
    , billing_agreements.signup_year

    , first_orders.first_menu_week_monday_date
    , first_orders.first_menu_week_week
    , first_orders.first_menu_week_month
    , first_orders.first_menu_week_quarter
    , first_orders.first_menu_week_year

    , case 
        when first_orders.source_created_at < billing_agreements.signup_at
        then first_orders.source_created_at
        else billing_agreements.signup_at 
        end as valid_from
    , {{ get_scd_valid_to() }} as valid_to

    from billing_agreements
    left join first_orders
    on billing_agreements.billing_agreement_id = first_orders.billing_agreement_id
    where valid_to is null

)

, scd_billing_agreements as (

    select
        billing_agreement_id
        , billing_agreement_status_id
        , sales_point_id
        , valid_from
        , {{ get_scd_valid_to('valid_to') }} as valid_to
    from billing_agreements
)

, scd_timeline_unified (

    select
        fixed_columns.billing_agreement_id,
        fixed_columns.valid_from
    from fixed_columns
    left join first_orders

    union
    
    select
        scd_billing_agreements.billing_agreement_id,
        scd_billing_agreements.valid_from
    from scd_billing_agreements

    /*union

    select
        scd_loyalty_level.billing_agreement_id,
        scd_loyalty_level.valid_from
    from scd_loyalty_level*/

)

, scd_timeline_valid_to_recalculated as (
    select
       billing_agreement_id
       , valid_from
       , {{ get_scd_valid_to('valid_from', 'billing_agreement_id') }} as valid_to
    from scd_timeline_unified
)

, tables_joined as (
select 
    md5(concat(
        cast(fixed_columns.billing_agreement_id as string),
        cast(scd_timeline_valid_to_recalculated.valid_from as string)
        )
    ) AS pk_dim_billing_agreements
    
    , fixed_columns.billing_agreement_id
    , fixed_columns.company_id
    , scd_billing_agreements.sales_point_id
    
    , scd_timeline_valid_to_recalculated.valid_from
    , scd_timeline_valid_to_recalculated.valid_to
    , case 
        when scd_timeline_valid_to_recalculated.valid_to = '9999-01-01'
        then true 
        else false 
    end as is_current

    , fixed_columns.payment_method
    , fixed_columns.signup_source
    , fixed_columns.signup_salesperson
    , status_names.billing_agreement_status_name
    {# TODO: must be swaped with the actual level name #}
    --, scd_loyalty_level.loyalty_level_id

    , fixed_columns.signup_date
    , fixed_columns.signup_year_day
    , fixed_columns.signup_month_day
    , fixed_columns.signup_week_day
    , fixed_columns.signup_week
    , fixed_columns.signup_month
    , fixed_columns.signup_quarter
    , fixed_columns.signup_year

    , fixed_columns.first_menu_week_monday_date
    , fixed_columns.first_menu_week_week
    , fixed_columns.first_menu_week_month
    , fixed_columns.first_menu_week_quarter
    , fixed_columns.first_menu_week_year

from scd_timeline_valid_to_recalculated
left join fixed_columns
    on scd_timeline_valid_to_recalculated.billing_agreement_id = fixed_columns.billing_agreement_id
    and scd_timeline_valid_to_recalculated.valid_from >= fixed_columns.valid_from
    and scd_timeline_valid_to_recalculated.valid_to <= fixed_columns.valid_to
left join scd_billing_agreements
    on scd_timeline_valid_to_recalculated.billing_agreement_id = scd_billing_agreements.billing_agreement_id
    and scd_timeline_valid_to_recalculated.valid_from >= scd_billing_agreements.valid_from
    and scd_timeline_valid_to_recalculated.valid_to <= scd_billing_agreements.valid_to
/*left join scd_loyalty_level
    on scd_timeline_valid_to_recalculated.billing_agreement_id = scd_loyalty_level.billing_agreement_id
    and scd_timeline_valid_to_recalculated.valid_from >= scd_loyalty_level.valid_from
    and scd_timeline_valid_to_recalculated.valid_to <= scd_loyalty_level.valid_to*/
left join status_names
    on scd_billing_agreements.billing_agreement_status_id = status_names.billing_agreement_status_id
)

select * from tables_joined