with 

source as (

    select * from {{ source('segment_net_backend_adams', 'ce_deviation_ordered') }}

),

renamed as (

    select
        id
        , received_at
        , products
        , action_done_by
        , cast(week as int) week
        , agreement_id
        , original_timestamp
        , sent_at
        , cast(year as int) as year
        , event_text
        , timestamp
        , user_id
        , taste_preferences
        , concept_preferences

    from source

)

select * from renamed