with 

source as (

    select * from {{ source('postgres', 'postgres_net_backend__ce_preferences_updated') }}

)

, columns_selected as (

        select 
            id
        , received_at
        , agreement_id
        , user_id
        , variation_id
        , concept_preference_name
        , sent_at
        , concept_preference_id
        , original_timestamp
        , taste_preferences
        , timestamp
        , updated_at
        , variation_name
        , action_done_by
        , concept_preferences

    from source

)

select * from columns_selected