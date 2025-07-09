with orders_forecast as (
    select
        forecast_job_run_metadata_id,
        menu_year,
        menu_week,
        company_id,
        forecast_group_id,
        forecast_order_quantity
    from prod.forecasting.forecast_orders
),

metadata_rank as (
    select
        forecast_job_run_metadata_id,
        row_number()over(
            partition by
                company_id,
                forecast_group_id,
                menu_year,
                menu_week
            order by created_at desc) as rank
    from prod.forecasting.forecast_job_run_metadata
    where forecast_model_id  = '2159EFBA-A366-49D9-98FD-9255E65F8B9D' -- total orders Analyst

),

latest_run as (
    select forecast_job_run_metadata_id from metadata_rank where rank = 1
),

orders_forecast_latest as (
    select
        orders_forecast.*
    from latest_run
    left join
        orders_forecast
        on orders_forecast.forecast_job_run_metadata_id = latest_run.forecast_job_run_metadata_id
),

dim_companies as (
    select company_id, company_name from prod.gold.dim_companies
),

aggregated as (
    select
        company_id,
        menu_year,
        menu_week,
        sum(forecast_order_quantity) as num_orders
    from orders_forecast_latest
    group by all
),

final as (
    select company_name, aggregated.*
    from aggregated
    left join dim_companies
        on dim_companies.company_id = aggregated.company_id
)

select
    *
from final
order by
    menu_year, menu_week
