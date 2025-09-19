with

budget as (
    select * from {{ ref('analyticsdb_shared__budget') }}
)

, dates as (
    select * from {{ ref('dim_dates') }}
)

, acquisition_target as(
    select * from {{ ref('analyticsdb_shared__budget_acquisition_targets') }}
)

-- Aggregate acquisition targets to monthly level to match budget grain
, acquisition_target_aggregated as (
    select 
        company_id
        , budget_type_id
        , budget_segment_id
        , financial_year
        , financial_month_number
        , cast(sum(monthly_acquisition_target) as int) as monthly_acquisition_target
    from acquisition_target
    group by all
)

, budget_tables_joined as (
    select
        --PKs
        md5(concat(
            budget.budget_id
            , budget.company_id
            , budget.budget_type_id
            , budget.budget_segment_id
            , budget.financial_year
            , budget.financial_week
        )) as pk_fact_budget

        --IDs
        , budget.budget_id
        , budget.budget_type_id
        , budget.budget_segment_id
        , budget.company_id

        --DATES
        , budget.financial_year
        , budget.financial_month_number
        , budget.financial_week

        --NUMBERS
        , budget.budget_atv_gross_ex_vat
        , budget.budget_number_of_orders 
        , budget.budget_order_value_gross_ex_vat
        , acquisition_target_aggregated.monthly_acquisition_target

        --FKs
        , dates.pk_dim_dates as fk_dim_dates
        , md5(budget.company_id) as fk_dim_companies
        , md5(budget.budget_type_id) as fk_dim_budget_types 
    from budget
    left join dates 
        on budget.financial_year = dates.financial_year 
        and budget.financial_week = dates.financial_week
        and dates.day_of_week = 1
    left join acquisition_target_aggregated
        on budget.financial_year = acquisition_target_aggregated.financial_year
        and budget.budget_type_id = acquisition_target_aggregated.budget_type_id
        and budget.budget_segment_id = acquisition_target_aggregated.budget_segment_id
        and budget.company_id = acquisition_target_aggregated.company_id
        and budget.financial_month_number = acquisition_target_aggregated.financial_month_number
)

select * from budget_tables_joined