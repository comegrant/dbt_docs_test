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

, menu_weeks_cutoffs_added as ( 
    
    select
        menu_week_monday_date
        , menu_year
        , menu_week
        , menu_year * 100 + menu_week as menu_year_week 
        , company_id
        , true as has_passed_cutoff
        , min(order_creation_date) - interval '1 day' + interval '23 hours 59 minutes' as menu_week_cutoff_time
    from orders_agreements_joined
    group by all

)

, sequence_numbers_added as (

    select
        menu_week_monday_date
        , menu_year
        , menu_week
        , menu_year * 100 + menu_week as menu_year_week 
        , company_id
        , menu_week_cutoff_time
        , has_passed_cutoff 
        , row_number () over (partition by company_id order by menu_week_monday_date) as menu_week_sequence_number
    from menu_weeks_cutoffs_added

)

, previous_cutoffs_next_menu_week_added as (

    select
        menu_week_monday_date
        , menu_year
        , menu_week
        , menu_year_week 
        , company_id
        , menu_week_cutoff_time
        , has_passed_cutoff
        , menu_week_sequence_number
        , menu_week_sequence_number
        , lag(menu_week_cutoff_time) over (partition by company_id order by menu_week_sequence_number) as previous_menu_week_cutoff_time
        , dateadd(week,1,menu_week_monday_date) as next_menu_week_monday_date
    from sequence_numbers_added

)

, next_menu_year_menu_week_is_latest_cutoff_flag_added as (

    select 
        menu_week_monday_date
        , menu_year
        , menu_week
        , menu_year_week 
        , company_id
        , menu_week_cutoff_time
        , has_passed_cutoff
        , menu_week_sequence_number
        , previous_menu_week_cutoff_time
        , next_menu_week_monday_date
        , extract(yearofweek from next_menu_week_monday_date) as next_menu_year
        , extract(week from next_menu_week_monday_date) as next_menu_week
        -- flag the latest menu week passed cutoff for each company_id
        , case when company_id is null
            then false
            else row_number() over (partition by company_id order by has_passed_cutoff desc, menu_week_monday_date desc) = 1 
        end as is_latest_menu_week_passed_cutoff

    from previous_cutoffs_next_menu_week_added

)

select * from next_menu_year_menu_week_is_latest_cutoff_flag_added
