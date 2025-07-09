with budget_type_name AS (
    select
        budget_type_id,
        budget_type_name as budget_type
    from prod.gold.dim_budget_types
),

marketing_input AS (
    select
        company_id,
        budget_year,
        year,
        month,
        budget_parameter_split_id,
        source,
        budget_type_id,
        estimate
    from prod.bronze.analyticsdb_shared__budget_marketing_input
),

company_name AS (
    select
        company_id,
        company_name
    FROM prod.gold.dim_companies
),

segment_name AS (
    select
        id,
        name AS customer_segment
    from prod.bronze.analyticsdb_shared__budget_parameter_split
),

final as (
    select
        company_name,
        marketing_input.year,
        marketing_input.month,
        budget_type_name.budget_type,
        customer_segment,
        marketing_input.source,
        marketing_input.estimate
    from marketing_input
    left join
        company_name
        on
            company_name.company_id = marketing_input.company_id
    inner join
        budget_type_name
        on
            budget_type_name.budget_type_id = marketing_input.budget_type_id
    left join
        segment_name
        on
            segment_name.id = marketing_input.budget_parameter_split_id
    where marketing_input.estimate > 0
        and budget_year = {budget_year}
        and budget_type = '{budget_type}'
)
