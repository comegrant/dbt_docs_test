with

job_run_metadata_distinct_dimensions as (

    select distinct

        forecast_job_id
        , company_id
        , forecast_group_id
        , forecast_model_id
        , forecast_horizon_index
        , forecast_horizon

    from {{ ref('forecasting__forecast_job_run_metadata') }}

)

, forecast_jobs as (

    select * from {{ ref('forecasting__forecast_jobs') }}

)

, forecast_groups as (

    select * from {{ ref('forecasting__forecast_groups') }}

)


, forecast_models as (

    select * from {{ ref('forecasting__forecast_models') }}

)

, forecast_dimension_data_joined as (

    select

        forecast_jobs.forecast_job_id
        , job_run_metadata_distinct_dimensions.company_id
        , forecast_groups.forecast_group_id
        , forecast_models.forecast_model_id

        , job_run_metadata_distinct_dimensions.forecast_horizon_index
        , job_run_metadata_distinct_dimensions.forecast_horizon
        , case 
            when job_run_metadata_distinct_dimensions.forecast_horizon = 1 then '1'
            else '11/15'
            end as forecast_horizon_group

        , forecast_jobs.forecast_job_name
        , forecast_groups.forecast_group_name
        , forecast_models.forecast_model_name

    from job_run_metadata_distinct_dimensions

    left join forecast_jobs
        on job_run_metadata_distinct_dimensions.forecast_job_id = forecast_jobs.forecast_job_id

    left join forecast_groups
        on job_run_metadata_distinct_dimensions.forecast_group_id = forecast_groups.forecast_group_id

    left join forecast_models
        on job_run_metadata_distinct_dimensions.forecast_model_id = forecast_models.forecast_model_id


)

, add_pk as (

    select
        md5(concat(
            forecast_job_id,
            company_id,
            forecast_group_id,
            forecast_model_id,
            forecast_horizon_index,
            forecast_horizon
        )) as pk_dim_forecast_runs
        , *

    from forecast_dimension_data_joined

)

select * from add_pk
