{% set customer_journey_segments_start_date = "2023-01-01" %}

with

agreements_with_history as (

    select * from {{ ref('int_billing_agreements_statuses_scd2') }}

)

, distinct_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, first_orders as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

)

, delivery_week_order as (

    select * from {{ ref('int_historic_order_weeks_numbered') }}

)

, orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}
    where 
    order_type_id in ({{var("subscription_order_type_ids") | join(", ")}})
    and order_status_id in ({{var("finished_order_status_ids") | join(", ")}})

)

, agreements as (

    select 
        agreements_with_history.billing_agreement_id
        , agreements_with_history.billing_agreement_status_name
        , agreements_with_history.valid_from
        , agreements_with_history.valid_to
        , distinct_agreements.company_id
        , distinct_agreements.signup_date
        , first_orders.first_menu_week_monday_date
    from agreements_with_history
    left join distinct_agreements 
        on agreements_with_history.billing_agreement_id = distinct_agreements.billing_agreement_id
    left join first_orders
        on agreements_with_history.billing_agreement_id = first_orders.billing_agreement_id
    where billing_agreement_status_id != 40 -- need to add deleted customers to model at a later stage

)

, agreements_first_freeze_date as (

    select billing_agreement_id
            , min (valid_from) as first_freeze_date 
    from agreements 
    where billing_agreement_status_name= 'Freezed' 
    group by 1
)

, agreements_and_orders_joined as (

    select 
        distinct_agreements.billing_agreement_id
        , orders.menu_week_monday_date
        , orders.menu_year
        , orders.menu_week
        , distinct_agreements.company_id
    from orders
    left join distinct_agreements
        on orders.billing_agreement_id = distinct_agreements.billing_agreement_id
 where orders.menu_week_monday_date > DATEADD(week, -9, '{{customer_journey_segments_start_date}}') -- to have orders to base the initial segments on

)

, agreements_and_weeks_joined as (

    select 
        agreements.billing_agreement_id
        , delivery_week_order.menu_week_monday_date
        , delivery_week_order.company_id
        , delivery_week_order.menu_year
        , delivery_week_order.menu_week
        , delivery_week_order.menu_week_cutoff_time
        , agreements.signup_date
        , floor (datediff
                    (delivery_week_order.menu_week_monday_date, agreements.signup_date) 
                / 7) 
                as weeks_since_signup
        , floor (datediff
                    (delivery_week_order.menu_week_monday_date, agreements.first_menu_week_monday_date) 
                / 7) 
                as weeks_since_first_order
    from agreements
    left join delivery_week_order
        on agreements.company_id = delivery_week_order.company_id
        and agreements.valid_from <= delivery_week_order.menu_week_monday_date
        and agreements.valid_to > delivery_week_order.menu_week_monday_date
    where menu_week_monday_date >= '{{customer_journey_segments_start_date}}'

)

, orders_with_rownumber as (

    select
        agreements_and_orders_joined.billing_agreement_id
        , agreements_and_orders_joined.menu_week_monday_date
        , agreements_and_orders_joined.menu_year
        , agreements_and_orders_joined.menu_week
        , delivery_week_order.menu_week_cutoff_time
        , delivery_week_order.delivery_week_order
        , delivery_week_order.company_id
    from agreements_and_orders_joined
    left join delivery_week_order 
        on agreements_and_orders_joined.company_id = delivery_week_order.company_id
        and agreements_and_orders_joined.menu_week_monday_date = delivery_week_order.menu_week_monday_date
   
)

, orders_past_8_weeks as (
    select 
        orders_with_rownumber.billing_agreement_id
        , delivery_week_order.menu_year
        , delivery_week_order.menu_week
        , delivery_week_order.menu_week_monday_date
        , count (*) as number_of_orders
    from delivery_week_order
    left join orders_with_rownumber 
        on delivery_week_order.company_id = orders_with_rownumber.company_id
        and (delivery_week_order.delivery_week_order - 7) <= orders_with_rownumber.delivery_week_order
        and delivery_week_order.delivery_week_order >= orders_with_rownumber.delivery_week_order
    group by all

)

, orders_past_1_to_9_weeks as (
    select 
        orders_with_rownumber.billing_agreement_id
        , delivery_week_order.menu_year
        , delivery_week_order.menu_week
        , delivery_week_order.menu_week_monday_date
        , count (*) as number_of_orders
    from delivery_week_order
    left join orders_with_rownumber 
        on delivery_week_order.company_id = orders_with_rownumber.company_id
        and orders_with_rownumber.delivery_week_order >= (delivery_week_order.delivery_week_order - 8)
        and orders_with_rownumber.delivery_week_order <= (delivery_week_order.delivery_week_order - 1)
    group by all

)

, reactivation_agreements as (

    select
        orders_with_rownumber.billing_agreement_id
        , orders_with_rownumber.menu_week_monday_date as reactivation_date
        , orders_with_rownumber.menu_week as reactivation_week
        , orders_with_rownumber.menu_year as reactivation_year
        , company_id
    from orders_with_rownumber
    left join orders_past_1_to_9_weeks
        on orders_with_rownumber.billing_agreement_id = orders_past_1_to_9_weeks.billing_agreement_id
        and orders_with_rownumber.menu_year = orders_past_1_to_9_weeks.menu_year
        and orders_with_rownumber.menu_week = orders_past_1_to_9_weeks.menu_week
    where orders_past_1_to_9_weeks.number_of_orders is null

)

, agreement_reactivation_ranked as (
    select
        delivery_week_order.menu_week_monday_date,
        delivery_week_order.company_id,
        reactivation_agreements.billing_agreement_id,
        reactivation_agreements.reactivation_date,
        row_number() over (
            partition by reactivation_agreements.billing_agreement_id, delivery_week_order.menu_week_monday_date
            order by reactivation_agreements.reactivation_date desc
        ) as rownumber
    from delivery_week_order
    left join reactivation_agreements
        on delivery_week_order.company_id = reactivation_agreements.company_id
        and delivery_week_order.menu_week_monday_date >= reactivation_agreements.reactivation_date 

)

, latest_agreement_reactivation as (
    select 
        menu_week_monday_date
        , billing_agreement_id
        , reactivation_date
    from agreement_reactivation_ranked
    where rownumber = 1

)

, weeks_since_reactivation as (

    select 
        billing_agreement_id
        , menu_week_monday_date
        , reactivation_date
        , floor (datediff
                    (menu_week_monday_date, reactivation_date) 
                / 7) 
                as weeks_since_reactivation
    from latest_agreement_reactivation

)

, sub_segments as (
    select 
        agreements_and_weeks_joined.billing_agreement_id
        , agreements_and_weeks_joined.menu_week_monday_date
        , agreements_and_weeks_joined.menu_year
        , agreements_and_weeks_joined.menu_week
        , agreements_and_weeks_joined.menu_year*100 + agreements_and_weeks_joined.menu_week as menu_yearweek
        , agreements_and_weeks_joined.menu_week_cutoff_time
        , case 
        when agreements_and_weeks_joined.weeks_since_signup < 3 
            and weeks_since_first_order is null
            and agreements_first_freeze_date.first_freeze_date < agreements_and_weeks_joined.menu_week_monday_date
            then {{ var("customer_journey_sub_segment_ids")["regret"] }}
        when agreements_and_weeks_joined.weeks_since_signup < 3 
            and agreements_and_weeks_joined.weeks_since_first_order is null 
            then {{ var("customer_journey_sub_segment_ids")["pending_onboarding"] }}
        when agreements_and_weeks_joined.weeks_since_signup >= 3 
            and agreements_and_weeks_joined.weeks_since_first_order is null 
            then {{ var("customer_journey_sub_segment_ids")["regret"] }}
        when (agreements_and_weeks_joined.weeks_since_first_order < 3 
            and agreements_and_weeks_joined.weeks_since_first_order is not null) 
            then {{ var("customer_journey_sub_segment_ids")["onboarding"] }}
        when (agreements_and_weeks_joined.weeks_since_first_order < 7 
            and orders_past_8_weeks.number_of_orders <4) 
            then {{ var("customer_journey_sub_segment_ids")["onboarding"] }}
        when (weeks_since_reactivation.weeks_since_reactivation < 7 
            and orders_past_8_weeks.number_of_orders < 4 ) 
            then {{ var("customer_journey_sub_segment_ids")["reactivated"] }}
        when orders_past_8_weeks.number_of_orders < 4
            and agreements_and_weeks_joined.weeks_since_first_order >= 7 
            then {{ var("customer_journey_sub_segment_ids")["occasional"] }}
        when orders_past_8_weeks.number_of_orders >= 4 
            and orders_past_8_weeks.number_of_orders < 6 
            and (agreements_and_weeks_joined.weeks_since_first_order >= 7 
                or (agreements_and_weeks_joined.weeks_since_first_order < 7 and orders_past_8_weeks.number_of_orders >= 4))
            then {{ var("customer_journey_sub_segment_ids")["regular"] }}
        when orders_past_8_weeks.number_of_orders >= 6 
            and orders_past_8_weeks.number_of_orders < 8
            and (agreements_and_weeks_joined.weeks_since_first_order >= 7 
                or (agreements_and_weeks_joined.weeks_since_first_order < 7 and orders_past_8_weeks.number_of_orders >= 4))
            then {{ var("customer_journey_sub_segment_ids")["frequent"] }}
        when orders_past_8_weeks.number_of_orders >= 7
            and (agreements_and_weeks_joined.weeks_since_first_order >= 7 
                or (agreements_and_weeks_joined.weeks_since_first_order < 7 and orders_past_8_weeks.number_of_orders >= 4))
            then {{ var("customer_journey_sub_segment_ids")["always_on"] }}
        when (agreements_and_weeks_joined.weeks_since_first_order >= 7 and orders_past_8_weeks.number_of_orders is null) 
            then {{ var("customer_journey_sub_segment_ids")["churned"] }}
        else 
            0 --unknown
        end as sub_segment_id
    from agreements_and_weeks_joined
    left join orders_past_8_weeks 
        on agreements_and_weeks_joined.billing_agreement_id = orders_past_8_weeks.billing_agreement_id
        and agreements_and_weeks_joined.menu_week_monday_date = orders_past_8_weeks.menu_week_monday_date
    left join weeks_since_reactivation
        on agreements_and_weeks_joined.billing_agreement_id = weeks_since_reactivation.billing_agreement_id
        and agreements_and_weeks_joined.menu_week_monday_date = weeks_since_reactivation.menu_week_monday_date
    left join first_orders
        on agreements_and_weeks_joined.billing_agreement_id = first_orders.billing_agreement_id
    left join agreements_first_freeze_date
        on agreements_and_weeks_joined.billing_agreement_id = agreements_first_freeze_date.billing_agreement_id
    where agreements_and_weeks_joined.signup_date < agreements_and_weeks_joined.menu_week_monday_date -- might need to be adjusted when introducing partial signup

)

, agreement_previous_segment as (
    select
        billing_agreement_id
        , menu_week_monday_date
        , menu_week_cutoff_time
        , sub_segment_id
        , lag(sub_segment_id) over (
            partition by billing_agreement_id
            order by menu_week_monday_date
        ) as previous_sub_segment_id
    from sub_segments

)

, segment_with_valid_from as (
    select
        billing_agreement_id
        , sub_segment_id
        , menu_week_monday_date as menu_week_monday_date_from
        , menu_week_cutoff_time as menu_week_cutoff_date_from
    from agreement_previous_segment
    where previous_sub_segment_id is null
       or sub_segment_id != previous_sub_segment_id

)

, segment_valid_to_and_from as (
    select
        segment_with_valid_from.billing_agreement_id
        , segment_with_valid_from.sub_segment_id
        , segment_with_valid_from.menu_week_monday_date_from
        , segment_with_valid_from.menu_week_cutoff_date_from
        , coalesce(
            lead (menu_week_monday_date_from) over (
                partition by segment_with_valid_from.billing_agreement_id
                order by menu_week_monday_date_from
            ),
            '{{var("future_proof_date")}}'
         ) as menu_week_monday_date_to
        , coalesce(
            lead (menu_week_cutoff_date_from) over (
                partition by segment_with_valid_from.billing_agreement_id
                order by menu_week_cutoff_date_from
            ),
            '{{var("future_proof_date")}}'
         ) as menu_week_cutoff_date_to
    from segment_with_valid_from

)

select * from segment_valid_to_and_from