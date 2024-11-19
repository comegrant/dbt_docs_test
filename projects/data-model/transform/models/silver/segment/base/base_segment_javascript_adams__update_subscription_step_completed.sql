with

source as (

    select * from {{ source('segment__javascript_adams', 'update_subscription_step_completed') }}

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
