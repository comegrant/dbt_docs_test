with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders')}}
    
)

, calendar as (

    select * from {{ ref('int_dates_with_financial_periods')}}

)

, first_orders as (
    select 
        billing_agreement_id
        , min_by(source_created_at, menu_week_monday_date) as first_order_created_at
        , min(menu_week_monday_date) as first_menu_week_monday_date
        , min_by(cast(date_format(menu_week_monday_date, 'yyyyMMdd') as int), menu_week_monday_date) as datekey
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
        , calendar.financial_week as first_menu_week_week
        , calendar.financial_month_name as first_menu_week_month
        , calendar.financial_quarter as first_menu_week_quarter
        , calendar.financial_year as first_menu_week_year
    from first_orders
    left join calendar 
        on first_orders.datekey = calendar.pk_dim_dates

)

select * from first_orders_cohorts
