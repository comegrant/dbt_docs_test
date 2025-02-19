with

budget_types as (

    select * from {{ref("analyticsdb_shared__budget_types")}}

),

budget_tables_joined as (
    select
        md5(budget_types.budget_type_id
        ) as pk_dim_budget_types,
        budget_types.budget_type_id,
        budget_types.budget_type_name,
        budget_types.budget_type_description
    from
    budget_types
)

select * from budget_tables_joined