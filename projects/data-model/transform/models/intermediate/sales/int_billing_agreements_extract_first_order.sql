with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders')}}
    
)

-- TODO: Remove below when historic data has been added to orders
, orders_history as (

    select * from {{ ref('analyticsdb_orders__historical_orders')}}
    
)

, orders_history_unioned as (

    select
        billing_agreement_id
        , menu_week_monday_date
        , order_type_id
        , order_status_id
        , source_created_at
    from orders

    union all

    select
        billing_agreement_id
        , menu_week_monday_date
        , order_type_id
        , order_status_id
        -- set to null as source created at does not represent
        -- when the historical order was created
        , null as source_created_at
    from orders_history

)
-- TODO: Remove above when historic data has been added to orders

, first_orders as (
    select 
        billing_agreement_id
        , min(source_created_at) as source_created_at
        , min(menu_week_monday_date) as first_menu_week_monday_date
    from orders_history_unioned
    where order_type_id in ({{ var('subscription_order_type_ids') | join(', ') }})
    and order_status_id in ({{ var('finished_order_status_ids') | join(', ') }})
    group by
        1 
)

, cohorts_first_orders as (
    select 
        billing_agreement_id
        , source_created_at
        , first_menu_week_monday_date
        , extract('WEEK', first_menu_week_monday_date) as first_menu_week_week
        , extract('MONTH', first_menu_week_monday_date) as first_menu_week_month
        , extract('QUARTER', first_menu_week_monday_date) as first_menu_week_quarter
        , extract('YEAROFWEEK', first_menu_week_monday_date) as first_menu_week_year
    from first_orders

)


select * from cohorts_first_orders
