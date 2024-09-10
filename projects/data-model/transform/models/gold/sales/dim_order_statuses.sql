with 

order_statuses as (

    select * from {{ ref('cms__billing_agreement_order_status') }}

),

relevant_columns as (
    select
        md5(order_status_id) AS pk_dim_order_statuses
        , order_status_id
        , order_status_name
        , can_be_cancelled
    from order_statuses
)

select * from relevant_columns