with budget_agged as (
    select
        budget_type_id,
        budget_id,
        fk_dim_companies,
        fk_dim_budget_types,
        financial_year,
        financial_week,
        sum(budget_number_of_orders) as total_orders
    from gold.fact_budget
        group by all
),

budget_types as (
    select
        pk_dim_budget_types,
        budget_type_name
    from prod.gold.dim_budget_types
),

budget_updated as (
    select
        budget_id,
        source_updated_at
    from prod.silver.analyticsdb_shared__budget
),

calendar as (
    select distinct
        financial_year,
        financial_quarter,
        financial_month_number,
        financial_week
    from prod.gold.dim_dates
    where financial_year >= 2021
    and day_of_week = 1
),
dim_companies as (
    select
        pk_dim_companies,
        company_name
    from
    prod.gold.dim_companies
),

final as (
    select
        company_name,
        budget_agged.financial_year as year,
        budget_agged.financial_week as week,
        financial_quarter as quarter,
        total_orders,
        budget_type_name,
        extract(year from source_updated_at) as updated_year
    from
        budget_agged
    left join
        budget_types
        on budget_types.pk_dim_budget_types = budget_agged.fk_dim_budget_types
    left join
        calendar
        on budget_agged.financial_year = calendar.financial_year
        and budget_agged.financial_week = calendar.financial_week
    left join
        dim_companies
        on dim_companies.pk_dim_companies = budget_agged.fk_dim_companies
    left join budget_updated
        on budget_updated.budget_id = budget_agged.budget_id
    order by year, week
)

select * from final
where year >= {min_year}
