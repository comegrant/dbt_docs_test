with 
customer_journey_segments as (
    select * from {{ ref('int_customer_journey_segments') }}

)

, customer_journey_reactivations as (
    select billing_agreement_id
        , menu_week_monday_date_from
        , menu_week_cutoff_date_from
    from customer_journey_segments
    where sub_segment_id = {{ var("customer_journey_sub_segment_ids")["reactivated"] }}

)

, reactivations_with_valid_from_and_to as (

    select
        billing_agreement_id
        , menu_week_monday_date_from as menu_week_monday_date_from
        , menu_week_cutoff_date_from as menu_week_cutoff_date_from
        , {{ get_scd_valid_to('menu_week_monday_date_from ', 'billing_agreement_id') }} as menu_week_monday_date_to
        , {{ get_scd_valid_to('menu_week_cutoff_date_from ', 'billing_agreement_id') }} as menu_week_cutoff_date_to
        from customer_journey_reactivations

)

select * from reactivations_with_valid_from_and_to