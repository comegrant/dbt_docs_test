with

source as (

    select * from {{ source('forecasting','forecast_job_run_metadata') }}

)

, add_monday_dates as (

    select

        source.*
        , {{ get_iso_week_start_date(
            'cast(left(next_cutoff_menu_week,4) as int)',
            'cast(right(next_cutoff_menu_week,2) as int)')
            }} as menu_week_monday_date_next_cutoff

        , {{ get_iso_week_start_date('menu_year','menu_week') }} as menu_week_monday_date

    from source

)


, add_forecast_horizon_index as (

    select
        add_monday_dates.*
        , cast(
            datediff(
                week
                , menu_week_monday_date_next_cutoff
                , menu_week_monday_date
                ) + 1
            as int) as forecast_horizon_index

    from add_monday_dates

)

, add_most_recent_flags as (

    select
        add_forecast_horizon_index.*

        -- true/false flag if job run is the most recent run for the menu_week and its position in the forecast horizon (forecast_horizon_index)
        -- a reservation is also made for the one-week forecast vs the multi-week forecast. Both will have forecast_horizon_index = 1, both are wanted in reporting
        , row_number() over (
            partition by
                forecast_job_id
                , company_id
                , menu_year
                , menu_week
                , forecast_group_id
                , forecast_model_id
                , forecast_horizon_index
                , case when forecast_horizon = 1 then 'One-week Forecast' else 'Multi-week Forecast' end
            order by created_at desc
        ) = 1 as is_most_recent_menu_week_horizon_forecast

        -- true/false flag if job run is the latest run for the menu_week
        , row_number() over (
            partition by
                forecast_job_id
                , company_id
                , menu_year
                , menu_week
                , forecast_group_id
                , forecast_model_id
            order by created_at desc
        ) = 1 as is_most_recent_menu_week_forecast

    from add_forecast_horizon_index

)

, renamed as (

    select

        {# ids #}
        forecast_job_run_metadata_id
        , forecast_job_run_id
        , forecast_job_run_parent_id
        , forecast_job_id
        , company_id
        , forecast_group_id
        , forecast_model_id

        {# ints #}
        , menu_year
        , menu_week
        , next_cutoff_menu_week
        , forecast_horizon
        , forecast_horizon_index

        {# dates #}
        , menu_week_monday_date_next_cutoff
        , menu_week_monday_date

        {# booleans #}
        , is_most_recent_menu_week_horizon_forecast
        , is_most_recent_menu_week_forecast

        {# system #}
        , created_at as source_created_at

    from add_most_recent_flags

)

select * from renamed
