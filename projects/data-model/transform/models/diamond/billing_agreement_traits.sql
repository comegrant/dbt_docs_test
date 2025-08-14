with 

customer_journey_segments as (

    select * from {{ ref('dim_customer_journey_segments') }}

)

, billing_agreements_partnerships_rules_aggregated as (

    select * from {{ ref('int_billing_agreements_partnerships_rules_aggregated') }}

)

, billing_agreement_customer_journey_segments_scd1 as (

    select * from {{ ref('int_customer_journey_segments') }}
    where menu_week_cutoff_at_to = '{{ var("future_proof_date") }}'

)

, dim_billing_agreements_scd1 as (

    select * from {{ ref('dim_billing_agreements') }}
    where is_current = true
    and billing_agreement_status_name <> 'Deleted'
    and company_id in (
        {{var ('active_company_ids') | join(', ')}}
    )

)

, all_tables_joined as (
    
    select 
        dim_billing_agreements_scd1.billing_agreement_id
        , dim_billing_agreements_scd1.company_id
        -- partnerships
        , dim_billing_agreements_scd1.partnership_name
        , billing_agreements_partnerships_rules_aggregated.partnership_rules
        -- customer_journey_segments
        , customer_journey_segments.customer_journey_main_segment_name
        , customer_journey_segments.customer_journey_sub_segment_name
        -- get the date of the freshest data
        , greatest(
            dim_billing_agreements_scd1.valid_from
            , billing_agreement_customer_journey_segments_scd1.menu_week_cutoff_at_from
        ) as valid_from
    
    from dim_billing_agreements_scd1

    left join billing_agreements_partnerships_rules_aggregated
        on dim_billing_agreements_scd1.billing_agreement_id = billing_agreements_partnerships_rules_aggregated.billing_agreement_id

    left join billing_agreement_customer_journey_segments_scd1
        on dim_billing_agreements_scd1.billing_agreement_id = billing_agreement_customer_journey_segments_scd1.billing_agreement_id

    left join customer_journey_segments
        on billing_agreement_customer_journey_segments_scd1.sub_segment_id = customer_journey_segments.customer_journey_sub_segment_id

)

select * from all_tables_joined