with

segment_update_subscription_step_completed as (
    select * from {{ ref('segment_javascript__update_subscription_step_completed') }}
)

, postgres_update_subscription_step_completed as (
    select * from {{ ref('postgres_javascript__update_subscription_step_completed') }}
)

, update_subscription_step_completed_unioned as (
    -- First, take all segment data
    select * from segment_update_subscription_step_completed

    union all

    -- Then add postgres data only where we don't have segment data for that event
    select postgres.*
    from postgres_update_subscription_step_completed as postgres
    where not exists (
        select 1
        from segment_update_subscription_step_completed as segment
        where segment.event_id_segment = postgres.event_id_segment
    )
)

, step_completed_union_with_has_completed_quiz_flag as (
    select
        event_id_segment
        , billing_agreement_id
        , company_id
        , source_created_at_segment
        , 1 as has_completed_subscription_quiz
    from update_subscription_step_completed_unioned
    where changed_subscription_details in ("Choose mealbox preferences", "Choose mealbox")
    -- "Choose mealbox preferences" is for the subscription quiz after OneSub migration
    -- "Choose mealbox" is for the subscription quiz before OneSub migration
)

select * from step_completed_union_with_has_completed_quiz_flag
