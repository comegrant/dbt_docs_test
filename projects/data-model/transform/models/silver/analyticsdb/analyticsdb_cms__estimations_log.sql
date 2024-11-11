with base_estimations_log_history as (

    select * from {{ ref('base_analyticsdb_cms__estimations_log_history') }}

)

, base_estimations_log as (

    select * from {{ ref('base_analyticsdb_cms__estimations_log') }}

)


, unioned as (

    select *
    from base_estimations_log_history
    union all
    select *
    from base_estimations_log
)

select * from unioned
