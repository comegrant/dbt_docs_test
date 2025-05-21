with orders as (

    select * from {{ref('cms__billing_agreement_orders')}}

)

, agreements as (

    select * from {{ref('cms__billing_agreements')}}
    where valid_to = '{{ var("future_proof_date") }}'
)

, latest_menu_week as (
    
    select 
        agreements.company_id
        , max(menu_week_monday_date) as menu_week_monday_date
        , max(menu_year*100 + menu_week) as menu_year_week
    from orders
    left join agreements
        on orders.billing_agreement_id = agreements.billing_agreement_id
    where 
        order_type_id = '5F34860B-7E61-46A0-80F7-98DCDC53BA9E' -- Recurring
        and order_status_id in (
            '4508130E-6BA1-4C14-94A4-A56B074BB135' --Finished
            , '38A5CC25-A639-4433-A8E6-43FB35DABFD9' --Processing
        )
    group by 1

)

select * from latest_menu_week