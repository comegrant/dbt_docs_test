with

dim_companies as (
    select
        company_id
        , country_id
        , company_name
    from
        {{ ref('dim_companies') }}
    where company_id in ({{ var('active_company_ids') | join(', ') }})
)

, cutoff_calendar as (
    select
        cutoff_id
        , menu_year
        , menu_week
        , cutoff_at_local_time
    from {{ ref('operations__cutoff_calendar') }}
)


, cutoff_country as (
    select
        cutoff_id
        , country_id
    from {{ ref('operations__cutoffs') }}
)

, country_cutoff_calendar as (
    select
        country_id
        , menu_year
        , menu_week
        , cutoff_at_local_time
    from
        cutoff_country
    left join cutoff_calendar
        on cutoff_country.cutoff_id = cutoff_calendar.cutoff_id
    where cutoff_country.cutoff_id != '6EDC0000-1102-D276-4A05-08DC941AB3A6' -- exclude Godt Matlyst
)

, dim_products as (
    select
        product_type_id
        , product_type_name
        , product_variation_id
        , product_variation_name
    from
        {{ ref('dim_products') }}
)

, estimations_log as (
    select
        menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , product_variation_quantity
        , estimation_generated_at
    from {{ ref('estimations_log') }}
)

, estimations_dim_product_dim_company_joined as (
    select
        estimations_log.*
        , country_id
        , product_type_id
        , product_type_name
        , product_variation_name
    from dim_companies
    left join
        estimations_log
        on dim_companies.company_id = estimations_log.company_id
    left join
        dim_products
        on
            estimations_log.product_variation_id = dim_products.product_variation_id
)

, estimations_joined as (
    select
        estimations_dim_product_dim_company_joined.*
        , cutoff_at_local_time
        , round(timediff(hour, estimation_generated_at, cutoff_at_local_time)/24) as num_days_before_cutoff
    from estimations_dim_product_dim_company_joined
    left join
        country_cutoff_calendar
        on
            estimations_dim_product_dim_company_joined.country_id = country_cutoff_calendar.country_id
            and estimations_dim_product_dim_company_joined.menu_year = country_cutoff_calendar.menu_year
            and estimations_dim_product_dim_company_joined.menu_week = country_cutoff_calendar.menu_week
)

, estimations_aggregated as (
    select
        menu_year
        , menu_week
        , company_id
        , estimation_generated_at
        , cutoff_at_local_time
        , num_days_before_cutoff
        , date(estimation_generated_at)   as estimations_date
        , sum(product_variation_quantity) as total_orders_estimated
    from estimations_joined
    where
        product_type_id in ('{{ var("financial_product_type_id") }}', '{{ var("mealbox_product_type_id") }}')
        and datediff(cutoff_at_local_time, estimation_generated_at) <= 28
        and menu_year >= 2023
    group by
        menu_year
        , menu_week
        , company_id
        , estimation_generated_at
        , estimations_date
        , cutoff_at_local_time
        , num_days_before_cutoff
)

, flex_estimations as (
    select
        menu_year
        , menu_week
        , company_id
        , estimation_generated_at
        , num_days_before_cutoff
        , product_variation_quantity as flex_orders_estimated
    from estimations_joined
    where product_variation_id = '10000000-0000-0000-0000-000000000000'
    and billing_agreement_basket_deviation_origin_id = '25017D0E-F788-48D7-8DC4-62581D58B698' -- customers swap
)

, add_is_first_estimations_of_date as (
    select
        *
        , row_number() over (
            partition by
                company_id
                , estimations_date
                , menu_year
                , menu_week
            order by estimation_generated_at asc
        ) as rank
    from estimations_aggregated
)

, estimations_total_orders_first_of_day as (
    select add_is_first_estimations_of_date.*
    from add_is_first_estimations_of_date
    where add_is_first_estimations_of_date.rank = 1

)

, final as (
    select
        estimations_total_orders_first_of_day.* except (rank, estimations_date)
        , flex_orders_estimated
    from
        estimations_total_orders_first_of_day
    left join
        flex_estimations
        on
            estimations_total_orders_first_of_day.company_id = flex_estimations.company_id
            and estimations_total_orders_first_of_day.menu_year = flex_estimations.menu_year
            and estimations_total_orders_first_of_day.menu_week = flex_estimations.menu_week
            and estimations_total_orders_first_of_day.estimation_generated_at
            = flex_estimations.estimation_generated_at
)

select *
from
    final
order by menu_year, menu_week, estimation_generated_at
