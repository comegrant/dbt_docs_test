with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders')}}
    
)

, first_orders as (
    select 
        billing_agreement_id
        , min_by(source_created_at, menu_week_monday_date) as first_order_created_at
        , min(menu_week_monday_date) as first_menu_week_monday_date
    from orders
    where order_type_id in ({{ var('subscription_order_type_ids') | join(', ') }})
    and order_status_id in ({{ var('finished_order_status_ids') | join(', ') }})
    group by
        1 
)

, cohorts_first_orders as (
    select 
        billing_agreement_id
        , first_order_created_at
        , first_menu_week_monday_date
        , extract('WEEK', first_menu_week_monday_date) as first_menu_week_week
        , extract('MONTH', first_menu_week_monday_date) as first_menu_week_month
        , extract('QUARTER', first_menu_week_monday_date) as first_menu_week_quarter
        , extract('YEAROFWEEK', first_menu_week_monday_date) as first_menu_week_year
    from first_orders

)


select * from cohorts_first_orders
