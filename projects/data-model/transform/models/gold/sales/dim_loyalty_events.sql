with 

source as (

    select * from {{ ref('cms__loyalty_events') }}

)

, add_pk as (

    select
        md5(loyalty_event_id) as pk_dim_loyalty_events
        , source.*
    from source

)

, relevant_columns as (

    select
        pk_dim_loyalty_events
        , loyalty_event_id
        , loyalty_event_name
    from add_pk

)

select * from relevant_columns