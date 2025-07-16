with

job_run_metadata as (

    select * from {{ ref('forecasting__forecast_job_run_metadata') }}

)

, forecast_variations as (

    select * from {{ ref('forecasting__forecast_variations') }}

)

, external_estimates as (

    select * from {{ ref('forecasting__forecast_external_estimates') }}

)

, forecast_variations_metadata_joined_filtered as (

    select
        forecast_variations.*
        , job_run_metadata.menu_week_monday_date
        , job_run_metadata.forecast_job_run_id
        , job_run_metadata.forecast_job_id
        , job_run_metadata.forecast_job_run_parent_id
        , job_run_metadata.forecast_horizon_index
        , job_run_metadata.forecast_horizon
        , job_run_metadata.is_most_recent_menu_week_horizon_forecast
        , job_run_metadata.is_most_recent_menu_week_forecast

    from forecast_variations

    left join job_run_metadata
        on forecast_variations.forecast_job_run_metadata_id = job_run_metadata.forecast_job_run_metadata_id

    where job_run_metadata.is_most_recent_menu_week_horizon_forecast is true

)

, add_external_overwrites as (

    select

        forecast_variations_metadata_joined_filtered.forecast_variations_id
        , forecast_variations_metadata_joined_filtered.forecast_job_run_metadata_id
        , forecast_variations_metadata_joined_filtered.forecast_job_run_id
        , forecast_variations_metadata_joined_filtered.forecast_job_id
        , forecast_variations_metadata_joined_filtered.forecast_job_run_parent_id
        , forecast_variations_metadata_joined_filtered.menu_year
        , forecast_variations_metadata_joined_filtered.menu_week
        , forecast_variations_metadata_joined_filtered.menu_week_monday_date
        , forecast_variations_metadata_joined_filtered.company_id
        , forecast_variations_metadata_joined_filtered.forecast_group_id
        , forecast_variations_metadata_joined_filtered.forecast_model_id
        , forecast_variations_metadata_joined_filtered.product_variation_id
        , forecast_variations_metadata_joined_filtered.product_variation_quantity_forecast_analytics

        , coalesce(
            external_estimates.product_variation_quantity_forecast
            , forecast_variations_metadata_joined_filtered.product_variation_quantity_forecast_analytics
        ) as product_variation_quantity_forecast

        , forecast_variations_metadata_joined_filtered.forecast_horizon_index
        , forecast_variations_metadata_joined_filtered.forecast_horizon
        , forecast_variations_metadata_joined_filtered.source_created_at
        , forecast_variations_metadata_joined_filtered.is_most_recent_menu_week_horizon_forecast
        , forecast_variations_metadata_joined_filtered.is_most_recent_menu_week_forecast

    from forecast_variations_metadata_joined_filtered

    left join external_estimates
        on forecast_variations_metadata_joined_filtered.product_variation_id = external_estimates.product_variation_id
        and forecast_variations_metadata_joined_filtered.company_id = external_estimates.company_id
        and forecast_variations_metadata_joined_filtered.menu_year = external_estimates.menu_year
        and forecast_variations_metadata_joined_filtered.menu_week = external_estimates.menu_week

)

select * from add_external_overwrites
