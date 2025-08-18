{% set customer_journey_segments_start_date = "2023-01-01" %}

with

agreements_status_history as (

    select * from {{ ref('int_billing_agreements_statuses_scd2') }}

)

, distinct_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, agreements_first_orders as (

    select * from {{ ref('int_billing_agreements_extract_first_order') }}

)

, menu_weeks as (

    select * from {{ ref('int_historic_menu_weeks_numbered') }}

)

, orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}
    where
    order_type_id in ({{var("subscription_order_type_ids") | join(", ")}})
    and order_status_id in ({{var("finished_order_status_ids") | join(", ")}})

)

--------------------------------------------------------------------------------
-- PREPARE AGREEMENTS WITH RELEVANT INFO AND DATES
--------------------------------------------------------------------------------

, menu_weeks_add_previous_cutoff as (

    select
          company_id
        , menu_week_monday_date
        , menu_week_cutoff_time
        , lag(menu_week_cutoff_time) over (
            partition by company_id
            order by menu_week_sequence_number
        ) as previous_menu_week_cutoff_time
    from menu_weeks

)

, agreements_add_dates as (

    select
        agreements_status_history.billing_agreement_id
        , distinct_agreements.company_id

        , distinct_agreements.signup_date
        , {{ get_monday_date_of_date('distinct_agreements.signup_date') }} as signup_date_monday_date
        , agreements_first_orders.first_menu_week_monday_date

        , min(
            case
                when agreements_status_history.billing_agreement_status_id = 40
                then agreements_status_history.valid_from
            end
        ) as deleted_date

        -- Partial signup when the agreement gets "User" status
        , min(
            case
                when agreements_status_history.billing_agreement_status_id = 5
                then agreements_status_history.valid_from
            end
        ) as partial_signup_date

        -- Full signup when the agreement gets a status that is not User or Lead
        , min(
            case
                when
                    agreements_status_history.billing_agreement_status_id is not null
                    and agreements_status_history.billing_agreement_status_id not in (5,50)
                then agreements_status_history.valid_from
            end
        ) as full_signup_date

    from agreements_status_history

    left join distinct_agreements
        on agreements_status_history.billing_agreement_id = distinct_agreements.billing_agreement_id

    left join agreements_first_orders
        on agreements_status_history.billing_agreement_id = agreements_first_orders.billing_agreement_id

    group by all

)

----------------------------------------
-- JOIN AGREEMENTS WITH MENU WEEKS
----------------------------------------

-- This is the main table that contains one row for each week and agreement
, menu_weeks_agreements_joined as (

    select
        agreements_add_dates.billing_agreement_id
        , menu_weeks.menu_week_monday_date
        , menu_weeks.company_id
        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_cutoff_time
        , agreements_add_dates.signup_date_monday_date
        , first_possible_menu_week.menu_week_monday_date as first_possible_menu_week_monday_date
        , agreements_add_dates.deleted_date
        , agreements_add_dates.partial_signup_date
        , agreements_add_dates.full_signup_date
        , {{ get_monday_date_of_date('agreements_add_dates.partial_signup_date') }} as partial_signup_monday_date
        , {{ get_monday_date_of_date('agreements_add_dates.full_signup_date') }} as full_signup_monday_date

        , date_diff(week, first_possible_menu_week.menu_week_monday_date, menu_weeks.menu_week_monday_date) as weeks_since_first_possible_menu_week

        , case
            when agreements_add_dates.first_menu_week_monday_date > menu_weeks.menu_week_monday_date then null
            else date_diff(week, agreements_add_dates.first_menu_week_monday_date, menu_weeks.menu_week_monday_date)
          end as weeks_since_first_order

    from menu_weeks

    -- Join the menu weeks with all agreements that existed in the week in question, 
    -- i.e. where the agreements were created before the monday of the week, 
    -- and where the agreement was not deleted at the cutoff time of the week.
    left join agreements_add_dates
       on menu_weeks.company_id = agreements_add_dates.company_id
       and menu_weeks.menu_week_monday_date >= agreements_add_dates.signup_date_monday_date
       and menu_weeks.menu_week_cutoff_time < coalesce(agreements_add_dates.deleted_date, '{{var("future_proof_date")}}')

    -- The first possible menu week for an agreement, is the first week with cutoff time after the full signup date:
    left join menu_weeks as first_possible_menu_week
        on agreements_add_dates.company_id = first_possible_menu_week.company_id
        -- We don't have cutoff time for all weeks before 2022, and use the monday date instead.
        and 
            coalesce( 
                first_possible_menu_week.menu_week_cutoff_time
                , first_possible_menu_week.menu_week_monday_date 
            )  
            >= agreements_add_dates.full_signup_date
        and 
            coalesce( 
                first_possible_menu_week.previous_menu_week_cutoff_time
                , first_possible_menu_week.menu_week_monday_date - interval 1 week
            ) 
            < agreements_add_dates.full_signup_date

    where
        menu_weeks.menu_week_monday_date >= '{{customer_journey_segments_start_date}}'

)

----------------------------------------
-- GET ORDER INFORMATION FOR AGREEMENTS
----------------------------------------

, agreement_orders_add_menu_week_info as (

    select
        orders.billing_agreement_id
        , distinct_agreements.company_id
        , orders.menu_week_monday_date
        , orders.menu_year
        , orders.menu_week
        , menu_weeks.menu_week_cutoff_time
        , menu_weeks.menu_week_sequence_number
    from orders
    left join distinct_agreements
        on orders.billing_agreement_id = distinct_agreements.billing_agreement_id
    left join menu_weeks
        on distinct_agreements.company_id = menu_weeks.company_id
        and orders.menu_week_monday_date = menu_weeks.menu_week_monday_date
    where orders.menu_week_monday_date > DATEADD(week, -9, '{{customer_journey_segments_start_date}}') -- to have orders to base the initial segments on

)

-- Find number of orders in the past 8 weeks, for each billing agreement and week.
, orders_past_8_weeks as (
    select
        agreement_orders_add_menu_week_info.billing_agreement_id
        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_monday_date
        , count (*) as number_of_orders
    from menu_weeks
    left join agreement_orders_add_menu_week_info
        on menu_weeks.company_id                      =  agreement_orders_add_menu_week_info.company_id
        and menu_weeks.menu_week_sequence_number - 7 <=  agreement_orders_add_menu_week_info.menu_week_sequence_number
        and menu_weeks.menu_week_sequence_number     >=  agreement_orders_add_menu_week_info.menu_week_sequence_number
    group by all

)

----------------------------------------
-- HANDLE REACTIVATIONS
----------------------------------------


-- Find number of orders in the past 1 to 9 weeks, for each billing agreement and week.
-- This is used to find reactivations, i.e. when an agreement has no orders in past 1 to 9 week, but has an order in the current week.
, orders_past_1_to_9_weeks as (
    select
        agreement_orders_add_menu_week_info.billing_agreement_id
        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , menu_weeks.menu_week_monday_date
        , count (*) as number_of_orders
    from menu_weeks
    left join agreement_orders_add_menu_week_info
        on menu_weeks.company_id                      = agreement_orders_add_menu_week_info.company_id
        and menu_weeks.menu_week_sequence_number - 8 <= agreement_orders_add_menu_week_info.menu_week_sequence_number
        and menu_weeks.menu_week_sequence_number - 1 >= agreement_orders_add_menu_week_info.menu_week_sequence_number
    group by all

)

-- Find reactivated agreements, i.e. agreements that have no orders in the past 1 to 9 weeks, but have an order in the current week.
, reactivated_agreements as (

    select
          agreement_orders_add_menu_week_info.billing_agreement_id
        , agreement_orders_add_menu_week_info.menu_week_monday_date as reactivation_date
        , agreement_orders_add_menu_week_info.menu_week as reactivation_week
        , agreement_orders_add_menu_week_info.menu_year as reactivation_year
        , agreement_orders_add_menu_week_info.company_id
    -- All rows in this table represent agreements that had an order in the week in question
    from agreement_orders_add_menu_week_info
    left join orders_past_1_to_9_weeks
        on agreement_orders_add_menu_week_info.billing_agreement_id = orders_past_1_to_9_weeks.billing_agreement_id
        and agreement_orders_add_menu_week_info.menu_year = orders_past_1_to_9_weeks.menu_year
        and agreement_orders_add_menu_week_info.menu_week = orders_past_1_to_9_weeks.menu_week
    -- We only include the agreements that didn't have any orders in the 8 weeks before the week in question
    where orders_past_1_to_9_weeks.number_of_orders is null

)

, reactivations_ranked as (
    select
        menu_weeks.menu_week_monday_date
        , reactivated_agreements.billing_agreement_id
        , reactivated_agreements.reactivation_date
        , row_number() over (
            partition by reactivated_agreements.billing_agreement_id, menu_weeks.menu_week_monday_date
            order by reactivated_agreements.reactivation_date desc
        ) as reactivation_desc_rank
    from menu_weeks
    left join reactivated_agreements
        on menu_weeks.company_id = reactivated_agreements.company_id
        and menu_weeks.menu_week_monday_date >= reactivated_agreements.reactivation_date

)

, latest_reactivation as (
    select
        menu_week_monday_date
        , billing_agreement_id
        , reactivation_date
        , case
            when date_diff(week, reactivation_date, menu_week_monday_date) < 0 then null
            else date_diff(week, reactivation_date, menu_week_monday_date)
          end as weeks_since_reactivation
    from reactivations_ranked
    where reactivation_desc_rank = 1

)

----------------------------------------
-- CREATE SUB SEGMENTS
----------------------------------------

, sub_segments as (
    select

        menu_weeks_agreements_joined.billing_agreement_id
        , menu_weeks_agreements_joined.menu_week_monday_date
        , menu_weeks_agreements_joined.menu_year
        , menu_weeks_agreements_joined.menu_week
        , menu_weeks_agreements_joined.menu_year*100 + menu_weeks_agreements_joined.menu_week as menu_yearweek
        , menu_weeks_agreements_joined.menu_week_cutoff_time
        , menu_weeks_agreements_joined.deleted_date

        , case

            --TODO: Follow up in #temp_crm_segment_model to decide if someone can stay in the partial signup segment forever..
            when menu_weeks_agreements_joined.menu_week_monday_date >= menu_weeks_agreements_joined.partial_signup_monday_date
                and menu_weeks_agreements_joined.menu_week_monday_date < coalesce(menu_weeks_agreements_joined.full_signup_monday_date, '{{var("future_proof_date")}}')
                then {{ var("customer_journey_sub_segment_ids")["partial_signup"] }}

            when
                coalesce(menu_weeks_agreements_joined.weeks_since_first_possible_menu_week,0) < 3
                and menu_weeks_agreements_joined.menu_week_monday_date >= menu_weeks_agreements_joined.full_signup_monday_date
                and menu_weeks_agreements_joined.weeks_since_first_order is null
                then {{ var("customer_journey_sub_segment_ids")["pending_onboarding"] }}

            when menu_weeks_agreements_joined.weeks_since_first_possible_menu_week >= 3
                and menu_weeks_agreements_joined.weeks_since_first_order is null
                then {{ var("customer_journey_sub_segment_ids")["regret"] }}

            when (menu_weeks_agreements_joined.weeks_since_first_order < 7
                and orders_past_8_weeks.number_of_orders < 4)
                then {{ var("customer_journey_sub_segment_ids")["onboarding"] }}

            when (latest_reactivation.weeks_since_reactivation < 7
                and orders_past_8_weeks.number_of_orders < 4 )
                then {{ var("customer_journey_sub_segment_ids")["reactivated"] }}

            when orders_past_8_weeks.number_of_orders < 4
                and menu_weeks_agreements_joined.weeks_since_first_order >= 7
                then {{ var("customer_journey_sub_segment_ids")["occasional"] }}

            when orders_past_8_weeks.number_of_orders >= 4
                and orders_past_8_weeks.number_of_orders < 6
                then {{ var("customer_journey_sub_segment_ids")["regular"] }}

            when orders_past_8_weeks.number_of_orders >= 6
                and orders_past_8_weeks.number_of_orders < 8
                then {{ var("customer_journey_sub_segment_ids")["frequent"] }}

            when orders_past_8_weeks.number_of_orders >= 7
                then {{ var("customer_journey_sub_segment_ids")["always_on"] }}

            when (menu_weeks_agreements_joined.weeks_since_first_order >= 7 and orders_past_8_weeks.number_of_orders is null)
                then {{ var("customer_journey_sub_segment_ids")["churned"] }}

            else
                -1 --unknown

        end as sub_segment_id

    from menu_weeks_agreements_joined

    left join orders_past_8_weeks
        on menu_weeks_agreements_joined.billing_agreement_id = orders_past_8_weeks.billing_agreement_id
        and menu_weeks_agreements_joined.menu_week_monday_date = orders_past_8_weeks.menu_week_monday_date

    left join latest_reactivation
        on menu_weeks_agreements_joined.billing_agreement_id = latest_reactivation.billing_agreement_id
        and menu_weeks_agreements_joined.menu_week_monday_date = latest_reactivation.menu_week_monday_date

    left join agreements_first_orders
        on menu_weeks_agreements_joined.billing_agreement_id = agreements_first_orders.billing_agreement_id

)

--------------------------------------------------------------------------------
-- CONSOLIDATE PERIODS OF THE SAME SUB SEGMENT AND HANDLE SCD FEATURES
--------------------------------------------------------------------------------

, sub_segments_add_previous_segment as (
    select
        billing_agreement_id
        , menu_week_monday_date
        , menu_week_cutoff_time
        , deleted_date
        , sub_segment_id
        , lag(sub_segment_id) over (
            partition by billing_agreement_id
            order by menu_week_monday_date
        ) as previous_sub_segment_id
    from sub_segments

)

, sub_segment_add_valid_from as (
    select
        billing_agreement_id
        , sub_segment_id
        , menu_week_monday_date as menu_week_monday_date_from
        , menu_week_cutoff_time as menu_week_cutoff_at_from
        , deleted_date
    from sub_segments_add_previous_segment
    where previous_sub_segment_id is null
       or sub_segment_id != previous_sub_segment_id

)

, sub_segment_add_valid_to as (
    select
        sub_segment_add_valid_from.billing_agreement_id
        , sub_segment_add_valid_from.sub_segment_id
        , sub_segment_add_valid_from.menu_week_monday_date_from
        , sub_segment_add_valid_from.menu_week_cutoff_at_from

        , coalesce(
            lead (menu_week_monday_date_from) over (
                partition by sub_segment_add_valid_from.billing_agreement_id
                order by menu_week_monday_date_from
            ),
            least( to_date('{{var("future_proof_date")}}'), to_date(deleted_date) )
         ) as menu_week_monday_date_to

        , coalesce(
            lead (menu_week_cutoff_at_from) over (
                partition by sub_segment_add_valid_from.billing_agreement_id
                order by menu_week_cutoff_at_from
            ),
            least( to_timestamp('{{var("future_proof_date")}}'), deleted_date )
         ) as menu_week_cutoff_at_to

    from sub_segment_add_valid_from

)

select * from sub_segment_add_valid_to
