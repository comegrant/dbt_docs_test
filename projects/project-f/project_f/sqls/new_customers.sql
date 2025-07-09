with historic_order_weeks as (
    select
        *
    from
        prod.intermediate.int_historic_order_weeks_numbered
    where menu_week_monday_date > '2023-01-01'

)

, agreement_company as (
    select
        billing_agreement_id
        , company_id
        , first_menu_week_monday_date
    from
        prod.gold.dim_billing_agreements
    where valid_to = '9999-12-31'

)

, agreement_company_with_first_order as (
    select
        *
    from
        prod.gold.dim_billing_agreements
    where
        valid_to = '9999-12-31'

)

, fact_orders as (
    select
        *
    from gold.fact_orders
)

, orders as (

    select distinct
        billing_agreement_id
        , menu_week_monday_date
    from fact_orders

)

, first_menu_week_with_orders as (
    select
        agreement_company.billing_agreement_id
        , first_menu_week_monday_date
        , ceil(
            datediff(
                orders.menu_week_monday_date
                , agreement_company.first_menu_week_monday_date
            ) / 7
        ) as weeks_since_first_order
        , company_id
    from agreement_company
    left join orders
        on agreement_company.billing_agreement_id = orders.billing_agreement_id
        and agreement_company.first_menu_week_monday_date <= orders.menu_week_monday_date

)

, number_of_first_order_customers_per_cohort as (

    select
        count(distinct billing_agreement_id) as number_of_customers
        , first_menu_week_monday_date
        , first_menu_week_with_orders.company_id
        , historic_order_weeks.menu_year as first_menu_year
        , historic_order_weeks.menu_year as first_menu_week
    from
        first_menu_week_with_orders
    left join
        historic_order_weeks
        on first_menu_week_with_orders.company_id = historic_order_weeks.company_id
        and first_menu_week_with_orders.first_menu_week_monday_date = historic_order_weeks.menu_week_monday_date
    group by first_menu_week_monday_date
        , first_menu_week_with_orders.company_id
        , historic_order_weeks.menu_year
        , historic_order_weeks.menu_year

)

, weekly_orders_per_cohort as (
    select
        count(*) as number_of_orders
        , company_id
        , monday_date as menu_week_monday_date
        , first_menu_week_monday_date
        , dim_dates.financial_year as menu_year
        , dim_dates.financial_week as menu_week
        , weeks_since_first_order as cohort_week_order
    from
        first_menu_week_with_orders
    left join
        gold.dim_dates on first_menu_week_with_orders.first_menu_week_monday_date = dim_dates.date
    where
        weeks_since_first_order < 8
    group by
        company_id
        , monday_date
        , first_menu_week_monday_date
        , dim_dates.financial_year
        , dim_dates.financial_week
        , weeks_since_first_order

)

, final as (
    select
        dim_companies.company_name
        , concat(
            historic_order_weeks.menu_year,
            '_',
            lpad(cast(historic_order_weeks.menu_week as string), 2, '0')
        ) as start_delivery_year_week
        , 'Web' as source
        , 'yes' as is_with_discount
        , cohort_week_order + 1 as cohort_week_order
        , weekly_orders_per_cohort.menu_year as delivery_year
        , weekly_orders_per_cohort.menu_week as delivery_week
        , number_of_first_order_customers_per_cohort.number_of_customers as number_of_registrations
        , weekly_orders_per_cohort.number_of_orders
    from
        historic_order_weeks
    left join
        gold.dim_companies
        on historic_order_weeks.company_id = dim_companies.company_id
    left join
        number_of_first_order_customers_per_cohort
        on historic_order_weeks.company_id = number_of_first_order_customers_per_cohort.company_id
        and historic_order_weeks.menu_week_monday_date = number_of_first_order_customers_per_cohort.first_menu_week_monday_date
    left join
        weekly_orders_per_cohort
        on historic_order_weeks.menu_week_monday_date = weekly_orders_per_cohort.first_menu_week_monday_date
        and historic_order_weeks.company_id = weekly_orders_per_cohort.company_id
    where historic_order_weeks.menu_year >= 2023
)

select
    *
from final
order by
    delivery_year
    , delivery_week
    , start_delivery_year_week
    , company_name
