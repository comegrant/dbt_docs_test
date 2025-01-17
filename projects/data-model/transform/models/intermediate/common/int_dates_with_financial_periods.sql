with 

dates as (
    select * from silver.databricks__dates
)

, find_corresponding_monday_date as (
    
    select 
        *
        , {{ get_iso_week_start_date('year_of_week', 'week') }} as monday_date
    from dates
)

, add_financial_periods as (

    select 
        pk_dim_dates
        , date
        , year as calendar_year

        , year_of_week as year_of_calendar_week
        , quarter as calendar_quarter
        , month_number as calendar_month_number
        , month_name as calendar_month_name
        , week as calendar_week
        
        , year(monday_date) as financial_year
        , quarter(monday_date) as financial_quarter
        , month(monday_date) as financial_month_number
        , date_format(monday_date, 'MMMM') as financial_month_name
        , case
            when month(monday_date) = 12 and week = 1 then 53
            else week
        end as financial_week

        , day_of_week
        , weekday_name
        , monday_date
        , monday_date - interval 1 week as monday_date_previous_week

    from find_corresponding_monday_date

)

-- Moving week 40 in 2024. This is how finance want the weeks, since it will then be 13 weeks in each quarter (except Q4 in 2024). 
-- TODO: We might need to do something in year 2030 and the following years
, moving_specific_weeks as (
    
    select 
        pk_dim_dates
        , date
        , calendar_year
        , year_of_calendar_week
        , calendar_quarter
        , calendar_month_number
        , calendar_month_name
        , calendar_week
        , financial_year
        , case
            when financial_year = 2024 and financial_week = 40 then 4
            else financial_quarter
        end as financial_quarter
        , case
            when financial_year = 2024 and financial_week = 40 then 10
            else financial_month_number
        end as financial_month_number
        , case
            when financial_year = 2024 and financial_week = 40 then 'October'
            else financial_month_name
        end as financial_month_name
        , financial_week
        , day_of_week
        , weekday_name
        , monday_date
        , monday_date_previous_week

    from add_financial_periods

)

select * from moving_specific_weeks