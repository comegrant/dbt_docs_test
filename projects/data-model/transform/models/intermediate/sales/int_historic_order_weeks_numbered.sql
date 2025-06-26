with

orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}
    where 
    order_type_id in ({{var("subscription_order_type_ids") | join(", ")}})
    and order_status_id in ({{var("finished_order_status_ids") | join(", ")}})
)

, agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, agreement_orders as (

    select 
        cast (convert_timezone('UTC', 'Europe/Oslo', orders.source_created_at) as date) as order_creation_date
        , orders.menu_week_monday_date
        , orders.menu_year
        , orders.menu_week
        , agreements.company_id
        , orders.source_created_at as order_created_at
    from orders
    left join agreements 
        on agreements.billing_agreement_id = orders.billing_agreement_id

)

, order_weeks_min_order_creation_date as ( 
    
    select
        min(order_creation_date) - interval '1 day' as order_cutoff_date
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
    from agreement_orders
    group by all

)

, order_weeks_numbered as (
    select 
        order_cutoff_date
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
        , row_number () over (partition by company_id order by menu_week_monday_date) as delivery_week_order
    from order_weeks_min_order_creation_date

)

, orders_with_cutoff as (
    select 
        order_cutoff_date + INTERVAL '23 hours 59 minutes' as menu_week_cutoff_time
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
        , delivery_week_order
    from order_weeks_numbered

)

select * from orders_with_cutoff