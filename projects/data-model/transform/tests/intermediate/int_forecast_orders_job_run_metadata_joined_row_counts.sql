with

forecast_orders as (

    select * from {{ ref('int_forecast_orders_job_run_metadata_joined') }}

)

, partitions_with_more_than_one_row as (

    select
        company_id
        , forecast_group_id
        , forecast_model_id
        , menu_year
        , menu_week
        , forecast_horizon_index
        , forecast_horizon
        , count(*) as nrow

    from forecast_orders
    where is_most_recent_menu_week_horizon_forecast is true
    group by all
    having count(*) > 1

)

select *
from partitions_with_more_than_one_row
