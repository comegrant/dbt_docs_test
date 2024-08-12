with 

order_types as (

    select * from {{ ref('sil_cms__order_types') }}

),


relevant_columns as (
    select
        md5(order_type_id) AS pk_dim_order_types
        , order_type_id
        , order_type_name
        , is_direct_order
        , allows_anonymous
        , is_direct_payment
    from order_types
)

select * from relevant_columns