with

update_subscription_step_completed_godtlevert as (

    select * from {{ ref('base_segment_javascript_godtlevert__update_subscription_step_completed') }}

)

, update_subscription_step_completed_adams as (

    select * from {{ ref('base_segment_javascript_adams__update_subscription_step_completed') }}

)

, update_subscription_step_completed_linas as (

    select * from {{ ref('base_segment_javascript_linas__update_subscription_step_completed') }}

)

, update_subscription_step_completed_retnemt as (

    select * from {{ ref('base_segment_javascript_retnemt__update_subscription_step_completed') }}

)

, update_subscription_step_completed_unioned as (

    select
        *
        , '09ECD4F0-AE58-4539-8E8F-9275B1859A19' as company_id
    from update_subscription_step_completed_godtlevert

    union all

    select
        *
        , '8A613C15-35E4-471F-91CC-972F933331D7' as company_id
    from update_subscription_step_completed_adams

    union all

    select
        *
        , '6A2D0B60-84D6-4830-9945-58D518D27AC2' as company_id
    from update_subscription_step_completed_linas

    union all

    select
        *
        , '5E65A955-7B1A-446C-B24F-CFE576BF52D7' as company_id
    from update_subscription_step_completed_retnemt

)

, renamed as (

    select

    {# ids #}
        id          as event_id_segment
        , user_id   as billing_agreement_id
        , company_id

        {# strings #}
        , changed_subscription_details

        {# dates #}
        , timestamp as source_created_at_segment

    from update_subscription_step_completed_unioned

)

select * from renamed
