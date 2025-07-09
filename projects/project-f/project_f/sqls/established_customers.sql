with menu_weeks as (
    select
        menu_week_monday_date
        , menu_year
        , menu_week
        , company_id
    from
        prod.intermediate.int_historic_order_weeks_numbered
    where
        menu_week_monday_date > '2023-01-01'

)

, orders as (
    select distinct
        company_id
        , billing_agreement_id
        , menu_week_monday_date
        , menu_year
        , menu_week
    from prod.gold.fact_orders

)

, agreement_company as (
    select
        billing_agreement_id
        , company_id
        , signup_year
    from
        prod.silver.cms__billing_agreements
    where
        valid_to = '9999-12-31'

)

, segments as (
    select
        *
    from
        prod.intermediate.int_customer_journey_segments

)

, segments_with_company as (
    select
        segments.billing_agreement_id
        , segments.sub_segment_id
        , segments.menu_week_monday_date_from
        , segments.menu_week_monday_date_to
        , company_id
        , signup_year
    from
        segments
    left join
        agreement_company
        on segments.billing_agreement_id = agreement_company.billing_agreement_id

)

,  orders_and_segments_joined as (
    select
        orders.billing_agreement_id
        , orders.company_id
        , customer_journey_sub_segment_name
        , orders.menu_year
        , orders.menu_week
        , segments_with_company.signup_year
    from
        orders
    left join
        segments_with_company
        on orders.billing_agreement_id = segments_with_company.billing_agreement_id
        and orders.menu_week_monday_date >= segments_with_company.menu_week_monday_date_from
        and orders.menu_week_monday_date < segments_with_company.menu_week_monday_date_to
    left join
        prod.gold.dim_customer_journey_segments
        on segments_with_company.sub_segment_id = gold.dim_customer_journey_segments.customer_journey_sub_segment_id
    where sub_segment_id >= 6

)

, number_of_orders_per_segment as (
    select
        count (*) as number_of_orders
        , company_id
        , signup_year
        , customer_journey_sub_segment_name
        , menu_year
        , menu_week
    from orders_and_segments_joined
    group by
        company_id
        , signup_year
        , customer_journey_sub_segment_name
        , menu_year
        , menu_week
)

,  segments_and_weeks_joined as (
    select
        billing_agreement_id
        , menu_weeks.company_id
        , customer_journey_sub_segment_name
        , menu_week_monday_date_from
        , menu_week_monday_date_to
        , menu_weeks.menu_year
        , menu_weeks.menu_week
        , signup_year
    from
        menu_weeks
    left join
        segments_with_company
        on menu_weeks.company_id = segments_with_company.company_id
        and menu_weeks.menu_week_monday_date >= segments_with_company.menu_week_monday_date_from
        and menu_weeks.menu_week_monday_date < segments_with_company.menu_week_monday_date_to
    left join
        gold.dim_customer_journey_segments
        on segments_with_company.sub_segment_id = gold.dim_customer_journey_segments.customer_journey_sub_segment_id
    where sub_segment_id >= 6
)

, number_of_agreements_per_segment as (
    select
        count (distinct billing_agreement_id) as number_of_agreements
        , company_id
        , signup_year
        , customer_journey_sub_segment_name
        , menu_year
        , menu_week
    from segments_and_weeks_joined
    group by
        company_id
        , signup_year
        , customer_journey_sub_segment_name
        , menu_year
        , menu_week
)

, final as (
    select
        company_name
        , menu_weeks.menu_year as current_delivery_year
        , menu_weeks.menu_week as current_delivery_week
        , number_of_agreements_per_segment.signup_year
        , number_of_agreements_per_segment.customer_journey_sub_segment_name
        , number_of_orders
        , number_of_agreements
    from
        menu_weeks
    left join
        gold.dim_companies on menu_weeks.company_id = dim_companies.company_id
    left join
        number_of_agreements_per_segment
        on number_of_agreements_per_segment.company_id = menu_weeks.company_id
        and number_of_agreements_per_segment.menu_year = menu_weeks.menu_year
        and number_of_agreements_per_segment.menu_week = menu_weeks.menu_week
    left join number_of_orders_per_segment
        on number_of_orders_per_segment.company_id = menu_weeks.company_id
        and number_of_orders_per_segment.menu_year = menu_weeks.menu_year
        and number_of_orders_per_segment.menu_week = menu_weeks.menu_week
        and number_of_orders_per_segment.customer_journey_sub_segment_name = number_of_agreements_per_segment.customer_journey_sub_segment_name
        and number_of_orders_per_segment.signup_year = number_of_agreements_per_segment.signup_year
    where menu_week_monday_date >= '2023-01-01'

)

select
    *
from final
where signup_year != 1970
