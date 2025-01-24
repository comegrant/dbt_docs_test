with 

loyalty_order_statuses as (

    select * from {{ ref('cms__loyalty_order_statuses') }}

),

relevant_columns as (
    select
        md5(cast(loyalty_order_status_id as string)) AS pk_dim_loyalty_order_statuses
        , loyalty_order_status_id
        , loyalty_order_status_name
    from loyalty_order_statuses
)

select * from relevant_columns