with

billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, int_subscription_step_has_started_subscription_quiz as (

    select * from {{ ref('int_subscription_step_has_started_subscription_quiz') }}

)

, int_subscription_step_has_completed_subscription_quiz as (

    select * from {{ ref('int_subscription_step_has_completed_subscription_quiz') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}
  
)

, update_subscription_events_unioned as (
    select
        event_id_segment
        , billing_agreement_id
        , company_id
        , source_created_at_segment
        , has_started_subscription_quiz
        , 0 as has_completed_subscription_quiz
    from int_subscription_step_has_started_subscription_quiz

    union all

    select
        event_id_segment
        , billing_agreement_id
        , company_id
        , source_created_at_segment
        , 0 as has_started_subscription_quiz
        , has_completed_subscription_quiz
    from int_subscription_step_has_completed_subscription_quiz
)

, fact_subscription_quiz as (

    select
        {# PK #}
        md5(concat_ws(
            '-'
            , update_subscription_events_unioned.event_id_segment
            , update_subscription_events_unioned.billing_agreement_id
            , update_subscription_events_unioned.company_id
            , update_subscription_events_unioned.has_started_subscription_quiz
            , update_subscription_events_unioned.has_completed_subscription_quiz
            , update_subscription_events_unioned.source_created_at_segment
        ))                                           as pk_fact_subscription_quiz

        , update_subscription_events_unioned.event_id_segment
        , update_subscription_events_unioned.billing_agreement_id
        , update_subscription_events_unioned.company_id
        , update_subscription_events_unioned.source_created_at_segment
        
        {# FKS #}
        , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , md5(update_subscription_events_unioned.company_id) as fk_dim_companies
        , cast(date_format(update_subscription_events_unioned.source_created_at_segment, 'yyyyMMdd') as int) as fk_dim_date_source_created_at_segment
        , cast(date_format(update_subscription_events_unioned.source_created_at_segment, 'HHmm') as int) as fk_dim_time_source_created_at_segment

        {# NUMBERS #}
        , update_subscription_events_unioned.has_started_subscription_quiz
        , update_subscription_events_unioned.has_completed_subscription_quiz


    from update_subscription_events_unioned
    left join billing_agreements
        on
            update_subscription_events_unioned.billing_agreement_id = billing_agreements.billing_agreement_id
            and update_subscription_events_unioned.source_created_at_segment >= billing_agreements.valid_from
            and update_subscription_events_unioned.source_created_at_segment < billing_agreements.valid_to
    left join billing_agreement_preferences
        on billing_agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id
    where billing_agreements.pk_dim_billing_agreements is not null
)

select * from fact_subscription_quiz
