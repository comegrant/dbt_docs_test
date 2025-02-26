with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders')}}
    
)

, dates as (

    select * from {{ ref('data_platform__dates')}}

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

, first_orders_cohorts as (
    select 
        billing_agreement_id
        , first_order_created_at
        , first_menu_week_monday_date
        , dates.financial_week as first_menu_week_week
        , dates.financial_month_name as first_menu_week_month
        , dates.financial_quarter as first_menu_week_quarter
        , dates.financial_year as first_menu_week_year
    from first_orders
    left join dates 
        on first_orders.first_menu_week_monday_date = dates.date

)

select * from first_orders_cohorts
