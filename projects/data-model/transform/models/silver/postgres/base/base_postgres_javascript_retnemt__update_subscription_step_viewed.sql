with

source as (

    select * from {{ source('postgres', 'postgres_javascript_retnemt__update_subscription_step_viewed') }}

)

, columns_selected as (

    select
        id
        , user_id
        , change_subscription_details
        , timestamp
    from source

)

select * from columns_selected
