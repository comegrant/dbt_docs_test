with

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, historical_statuses as (

    select * from {{ ref('int_billing_agreements_historical_statuses') }}
    
)

, status_names as (

    select * from {{ ref('cms__billing_agreement_statuses') }}

)

, union_with_historical_statuses as (

    select 
        billing_agreement_id
        , billing_agreement_status_id
        , valid_from

    from billing_agreements

    union all

    select 
        billing_agreement_id
        , billing_agreement_status_id
        , valid_from

    from historical_statuses

)

, add_valid_to_and_status_name as (

    select 
        union_with_historical_statuses.billing_agreement_id
        , status_names.billing_agreement_status_name
        , union_with_historical_statuses.valid_from
        , {{get_scd_valid_to('union_with_historical_statuses.valid_from', 'billing_agreement_id')}} as valid_to
        
    from union_with_historical_statuses
    left join status_names
        on union_with_historical_statuses.billing_agreement_status_id = status_names.billing_agreement_status_id

)

select * from add_valid_to_and_status_name