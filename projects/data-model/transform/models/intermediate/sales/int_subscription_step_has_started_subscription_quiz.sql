with

segment_update_subscription_step_viewed as (
    select * from {{ ref('segment_javascript__update_subscription_step_viewed') }}
)

, postgres_update_subscription_step_viewed as (
    select * from {{ ref('postgres_javascript__update_subscription_step_viewed') }}
)

, update_subscription_step_viewed_unioned as (
    -- First, take all segment data
    select * from segment_update_subscription_step_viewed

    union all

    -- Then add postgres data only where we don't have segment data for that event
    select postgres.*
    from postgres_update_subscription_step_viewed as postgres
    where not exists (
        select 1
        from segment_update_subscription_step_viewed as segment
        where segment.event_id_segment = postgres.event_id_segment
    )
)

, step_viewed_union_with_has_started_quiz_flag as (
    select
        event_id_segment
        , billing_agreement_id
        , company_id
        , source_created_at_segment
        , 1 as has_started_subscription_quiz
    from update_subscription_step_viewed_unioned
    where change_subscription_details = "Choose mealbox"
)

select * from step_viewed_union_with_has_started_quiz_flag
