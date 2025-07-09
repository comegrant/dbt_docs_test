with historic_order_weeks as (
    select
        *
    from prod.intermediate.int_historic_order_weeks_numbered
    where menu_week_monday_date > '2022-12-01'

)

, agreement_company as (
    select
        billing_agreement_id
        , company_id
    from prod.silver.cms__billing_agreements
    where valid_to = '9999-12-31'
)

, reactivation_cohorts as (
    select
        *
    from prod.intermediate.int_reactivation_customer_cohorts

)

, fact_orders as (
    select
        *
    from gold.fact_orders

)

, reactivation_cohorts_with_company as (
    select
        reactivation_cohorts.billing_agreement_id
        , menu_week_monday_date_from as reactivation_monday_date_from
        , menu_week_monday_date_to as reactivation_monday_date_to
        , company_id
    from reactivation_cohorts
    left join
        agreement_company
        on reactivation_cohorts.billing_agreement_id = agreement_company.billing_agreement_id

)

, orders as (
    select distinct
        billing_agreement_id
        , menu_week_monday_date
    from fact_orders
)

, reactivations_with_orders as (
    select
        reactivation_cohorts_with_company.billing_agreement_id
        , reactivation_monday_date_from
        , menu_week_monday_date
        , ceil(
            datediff(
                orders.menu_week_monday_date
                , reactivation_cohorts_with_company.reactivation_monday_date_from
            ) / 7
        ) as weeks_since_reactivation
        , company_id
    from
        reactivation_cohorts_with_company
    left join
        orders
        on reactivation_cohorts_with_company.billing_agreement_id = orders.billing_agreement_id
        and reactivation_cohorts_with_company.reactivation_monday_date_from <= orders.menu_week_monday_date
        and reactivation_cohorts_with_company.reactivation_monday_date_to > orders.menu_week_monday_date

)

, number_of_reactivated_customers_per_cohort as (
    select
        count(distinct billing_agreement_id) as number_of_customers
        , reactivation_monday_date_from as reactivation_menu_week_monday_date
        , reactivations_with_orders.company_id
        , historic_order_weeks.menu_year as reactivation_year
        , historic_order_weeks.menu_week as reactivation_week
    from reactivations_with_orders
    left join historic_order_weeks
        on reactivations_with_orders.company_id = historic_order_weeks.company_id
        and reactivations_with_orders.reactivation_monday_date_from = historic_order_weeks.menu_week_monday_date
    group by reactivation_menu_week_monday_date
        , reactivations_with_orders.company_id
        , historic_order_weeks.menu_year
        , historic_order_weeks.menu_week

)

, weekly_orders_per_cohort as (
    select
        count(*) as number_of_orders
        , company_id
        , menu_week_monday_date
        , reactivation_monday_date_from as reactivation_menu_week_monday_date
        , dim_dates.financial_year as menu_year
        , dim_dates.financial_week as menu_week
        , weeks_since_reactivation as cohort_week_order
    from reactivations_with_orders
    left join
        prod.gold.dim_dates
        on reactivations_with_orders.menu_week_monday_date = dim_dates.date
    where weeks_since_reactivation < 8
    group by
        company_id
        , menu_week_monday_date
        , reactivation_monday_date_from
        , dim_dates.financial_year
        , dim_dates.financial_week
        , weeks_since_reactivation

)

, final as (
    select
        dim_companies.company_name
        , CONCAT(historic_order_weeks.menu_year, '_', LPAD(CAST(historic_order_weeks.menu_week AS STRING), 2, '0')) as start_delivery_year_week
        , number_of_reactivated_customers_per_cohort.reactivation_year
        , 'Web' as source --Web/Sales
        , 'yes' as is_with_discount --yes/no
        , cohort_week_order + 1 as cohort_week_order
        , weekly_orders_per_cohort.menu_year as delivery_year
        , weekly_orders_per_cohort.menu_week as delivery_week
        , sum(number_of_reactivated_customers_per_cohort.number_of_customers) as number_of_registrations
        , weekly_orders_per_cohort.number_of_orders
    from historic_order_weeks
    left join gold.dim_companies on historic_order_weeks.company_id = dim_companies.company_id
    left join number_of_reactivated_customers_per_cohort
        on historic_order_weeks.company_id = number_of_reactivated_customers_per_cohort.company_id
        and historic_order_weeks.menu_week_monday_date = number_of_reactivated_customers_per_cohort.reactivation_menu_week_monday_date
    left join weekly_orders_per_cohort
        on historic_order_weeks.menu_week_monday_date = weekly_orders_per_cohort.menu_week_monday_date
        and historic_order_weeks.company_id = weekly_orders_per_cohort.company_id
    where historic_order_weeks.menu_year >= 2022
    group by all
)

select
    *
from final
where start_delivery_year_week > '2023_01'
order by
    delivery_year
    , delivery_week
    , start_delivery_year_week
    , company_name
