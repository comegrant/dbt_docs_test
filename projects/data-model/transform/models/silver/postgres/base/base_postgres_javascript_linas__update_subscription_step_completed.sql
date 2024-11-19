with

source as (

    select * from {{ source('postgres', 'postgres_javascript_lmk__update_subscription_step_completed') }}

)

, columns_selected as (

    select
        id
        , user_id
        , changed_subscription_details
        , timestamp
    from source

)

select * from columns_selected
