with 

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

),

most_recent_billing_agreements as (

    select * from billing_agreements
    where valid_to is null

),

scd_billing_agreements as (

    select
        agreement_id
        , billing_agreement_status_id
        , sales_point_id
        , valid_from
        , coalesce(valid_to, cast('9999-01-01' as timestamp)) as valid_to
    from billing_agreements

),

billing_agreement_status as (

    select * from {{ ref('cms__billing_agreement_status') }}

),

first_order as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

),

scd_loyalty_level as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd') }}

),

union_scd_timeline_valid_from (

    select
        scd_billing_agreements.agreement_id,
        scd_billing_agreements.valid_from
    from scd_billing_agreements

    union

    select
        scd_loyalty_level.agreement_id,
        scd_loyalty_level.valid_from
    from scd_loyalty_level

),

union_scd_timeline_add_valid_to as (
    select
        agreement_id,
        valid_from,
        coalesce(lead(valid_from, 1) over (partition by agreement_id order by valid_from), 
        cast('9999-01-01' as timestamp)
        ) as valid_to
    from union_scd_timeline_valid_from
),

join_tables as (
select 
    md5(concat(
        cast(most_recent_billing_agreements.agreement_id as string),
        cast(union_scd_timeline_add_valid_to.valid_from as string)
        )
    ) AS pk_dim_billing_agreements
    
    {# ids #}
    , most_recent_billing_agreements.agreement_id
    , most_recent_billing_agreements.customer_id
    , most_recent_billing_agreements.company_id
    , scd_billing_agreements.sales_point_id
    --, most_recent_billing_agreements.payment_partner_id
    --, most_recent_billing_agreements.applicant_id
    --, most_recent_billing_agreements.trigger_registration_process as trigger_registration_process_id
    
    {# scd validity #}
    , union_scd_timeline_add_valid_to.valid_from
    , union_scd_timeline_add_valid_to.valid_to
    , case 
        when union_scd_timeline_add_valid_to.valid_to = '9999-01-01'
        then true 
        else false 
    end as is_current

    {# strings #}
    , most_recent_billing_agreements.payment_method
    , most_recent_billing_agreements.signup_source
    , most_recent_billing_agreements.signup_salesperson
    , billing_agreement_status.billing_agreement_status_name
    {# TODO: must be changed with the actual level name #}
    , scd_loyalty_level.loyalty_level_id

    {# booleans #}
    --, most_recent_billing_agreements.addresses_copied

    {# date #}
    , most_recent_billing_agreements.signup_date
    , most_recent_billing_agreements.signup_year_day
    , most_recent_billing_agreements.signup_month_day
    , most_recent_billing_agreements.signup_week_day
    , most_recent_billing_agreements.signup_week
    , most_recent_billing_agreements.signup_month
    , most_recent_billing_agreements.signup_quarter
    , most_recent_billing_agreements.signup_year

    , most_recent_billing_agreements.start_date
    , most_recent_billing_agreements.start_year_day
    , most_recent_billing_agreements.start_month_day
    , most_recent_billing_agreements.start_week_day
    , most_recent_billing_agreements.start_week
    , most_recent_billing_agreements.start_month
    , most_recent_billing_agreements.start_quarter
    , most_recent_billing_agreements.start_year

    , first_order.first_delivery_week_monday_date
    , first_order.first_delivery_week_week
    , first_order.first_delivery_week_month
    , first_order.first_delivery_week_quarter
    , first_order.first_delivery_week_year

from union_scd_timeline_add_valid_to
left join most_recent_billing_agreements
    on union_scd_timeline_add_valid_to.agreement_id = most_recent_billing_agreements.agreement_id
left join scd_billing_agreements
    on union_scd_timeline_add_valid_to.agreement_id = scd_billing_agreements.agreement_id
    and union_scd_timeline_add_valid_to.valid_from >= scd_billing_agreements.valid_from
    and union_scd_timeline_add_valid_to.valid_to < scd_billing_agreements.valid_to
left join billing_agreement_status
    on scd_billing_agreements.billing_agreement_status_id = billing_agreement_status.billing_agreement_status_id
left join scd_loyalty_level
    on union_scd_timeline_add_valid_to.agreement_id = scd_loyalty_level.agreement_id
    and union_scd_timeline_add_valid_to.valid_from >= scd_loyalty_level.valid_from
    and union_scd_timeline_add_valid_to.valid_to < scd_loyalty_level.valid_to
left join first_order
    on most_recent_billing_agreements.agreement_id = first_order.agreement_id
)

select * from join_tables