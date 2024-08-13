with 

orders as (

    select * from {{ ref('sil_cms__billing_agreement_orders')}}
    
)

, first_orders as (
    select 
        agreement_id
        , first(delivery_week_monday_date) as first_delivery_week_monday_date
    from orders
    group by
        1
)

, cohorts_first_orders as (
    select 
        agreement_id
        , first_delivery_week_monday_date
        , extract('WEEK', first_delivery_week_monday_date) as first_delivery_week_week
        , extract('MONTH', first_delivery_week_monday_date) as first_delivery_week_month
        , extract('QUARTER', first_delivery_week_monday_date) as first_delivery_week_quarter
        , extract('YEAROFWEEK', first_delivery_week_monday_date) as first_delivery_week_year
    from first_orders

)


select * from cohorts_first_orders