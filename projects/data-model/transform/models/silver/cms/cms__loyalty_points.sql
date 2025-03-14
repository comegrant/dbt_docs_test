{{
    config(
        materialized='incremental',
        unique_key='loyalty_points_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__loyalty_points') }}

)

, add_missing_event_ids as (

    select 
        *
        , case 
            -- Adding Order Was Cancelled By Logistics event ID on loyalty points where transaction reason is 'Orders was cancelled by logistics' but no event_id exists.
            when (event_id is null and loyalty_order_id is null and reason = 'Orders was cancelled by logistics') then upper('18b53be1-e812-49ed-be6e-52f4063292a7')
            -- Adding Expired Points Event ID on loyalty points where transaction reason is points expiry but no event_id exists.
            when (event_id is null and loyalty_order_id is null and reason = 'Expiration date reached') then upper('dcbb0afb-1cba-4d9b-ab1b-168afe98f100')
            -- Creating an event_id on points removed due to No orders placed in last 3 months (12 weeks). No event exists for these in source but reason is always the same. 
            when (event_id is null and loyalty_order_id is null and reason = 'No orders placed in last 3 months (12 weeks)') then '10000000-0000-0000-0000-000000000000'
            -- Creating an event_id on points spent on loyalty orders. No event exists for these in source but loyalty_order_id always exists.
            when (event_id is null and loyalty_order_id is not null) then '20000000-0000-0000-0000-000000000000'
            else event_id end as loyalty_event_id
    from source

)

, renamed as (

    select 

        {# ids #}
        id as loyalty_points_id
        , parent_id as loyalty_points_parent_id
        , agreement_id as billing_agreement_id
        , loyalty_event_id
        , loyalty_order_id
        , bao_id as billing_agreement_order_id
        
        {# strings #}
        , reason as transaction_reason
        
        {# ints #}
        , transaction_points
        , remaining_points
        
        {# timesamps #}
        , expiration_date as points_expiration_date
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
    from add_missing_event_ids

)

select * from renamed