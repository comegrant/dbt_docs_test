with

fact_partnership_points as (

    select * from {{ ref('fact_partnership_points') }}

)

, fact_orders as (

    select * from {{ ref('fact_orders') }}

)

, add_keys as (

    select
    -- pk
        md5(
            concat(
                fact_orders.pk_fact_orders
                , fact_partnership_points.fk_dim_partnerships
            )
        ) as pk_bridge_fact_orders_dim_partnerships

    --fks
        , fact_orders.pk_fact_orders as fk_fact_orders
        , fact_partnership_points.fk_dim_partnerships

    from fact_partnership_points

    left join fact_orders
        on fact_partnership_points.billing_agreement_order_id = fact_orders.billing_agreement_order_id

)

select * from add_keys
