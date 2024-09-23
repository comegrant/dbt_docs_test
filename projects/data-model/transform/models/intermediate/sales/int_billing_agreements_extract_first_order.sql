with 

orders as (

    select * from {{ ref('cms__billing_agreement_orders')}}
    
)

, first_orders as (
    select 
        billing_agreement_id
        , min(source_created_at) as source_created_at
        , min(menu_week_monday_date) as first_menu_week_monday_date
    from orders
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