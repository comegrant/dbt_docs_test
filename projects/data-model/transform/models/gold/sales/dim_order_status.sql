with 

order_status as (

    select * from {{ ref('sil_cms__order_status') }}

),

relevant_columns as (
    select
        md5(order_status_id) AS pk_dim_order_status
        , order_status_id
        , order_status_name
        , can_be_cancelled
    from order_status
)

select * from relevant_columns