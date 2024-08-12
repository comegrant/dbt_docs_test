with 

billing_agreements as (

    select * from {{ ref('sil_cms__billing_agreements') }}

),

billing_agreement_status as (

    select * from {{ ref('sil_cms__billing_agreement_status') }}

),


billing_agreements_first_order as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

),

join_tables as (
select 

    md5(cast(billing_agreements.agreement_id as string)) AS pk_dim_billing_agreements
    {# ids #}
    , billing_agreements.agreement_id
    , billing_agreements.customer_id
    , billing_agreements.company_id
    , billing_agreements.sales_point_id
    --, billing_agreements.payment_partner_id
    --, billing_agreements.applicant_id
    --, billing_agreements.trigger_registration_process as trigger_registration_process_id
    
    {# strings #}
    , billing_agreements.payment_method
    , billing_agreements.signup_source
    , billing_agreements.signup_salesperson
    , billing_agreement_status.billing_agreement_status_name

    {# booleans #}
    --, billing_agreements.addresses_copied

    {# date #}
    , billing_agreements.signup_date
    , billing_agreements.signup_isoyear_day
    , billing_agreements.signup_month_day
    , billing_agreements.signup_isoweek_day
    , billing_agreements.signup_isoweek
    , billing_agreements.signup_month
    , billing_agreements.signup_quarter
    , billing_agreements.signup_isoyear

    , billing_agreements.start_date
    , billing_agreements.start_isoyear_day
    , billing_agreements.start_month_day
    , billing_agreements.start_isoweek_day
    , billing_agreements.start_isoweek
    , billing_agreements.start_month
    , billing_agreements.start_quarter
    , billing_agreements.start_isoyear

    , billing_agreements_first_order.first_delivery_week_monday_date
    , billing_agreements_first_order.first_delivery_week_isoweek
    , billing_agreements_first_order.first_delivery_week_month
    , billing_agreements_first_order.first_delivery_week_quarter
    , billing_agreements_first_order.first_delivery_week_isoyear

    {# timestamp #}
    , billing_agreements.start_at

from billing_agreements
left join billing_agreements_first_order
on billing_agreements.agreement_id = billing_agreements_first_order.agreement_id
left join billing_agreement_status
on billing_agreements.billing_agreement_status_id = billing_agreement_status.billing_agreement_status_id
)

select * from join_tables