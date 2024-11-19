with

source as (

    select * from {{ source('segment__javascript_linas', 'update_subscription_step_viewed') }}

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
