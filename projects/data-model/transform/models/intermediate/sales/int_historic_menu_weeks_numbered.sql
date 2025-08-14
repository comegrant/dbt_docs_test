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

, orders_agreements_joined as (

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

, menu_weeks_with_cutoff as ( 
    
    select
        min(order_creation_date) - interval '1 day' + interval '23 hours 59 minutes' as menu_week_cutoff_time
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
    from orders_agreements_joined
    group by all

)

, menu_weeks_numbered as (
    select 
        menu_week_cutoff_time
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
        , row_number () over (partition by company_id order by menu_week_monday_date) as menu_week_sequence_number
    from menu_weeks_with_cutoff

)

, menu_weeks_numbered_add_previous_cutoff as (

    select
        menu_week_cutoff_time
        , menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
        , menu_week_sequence_number
        , lag(menu_week_cutoff_time) over (
            partition by company_id 
            order by menu_week_sequence_number
        ) as previous_menu_week_cutoff_time
    from menu_weeks_numbered
)

select * from menu_weeks_numbered_add_previous_cutoff