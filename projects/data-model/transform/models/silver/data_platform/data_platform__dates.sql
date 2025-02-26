with 

dates as (
    select * from {{ source('data_platforms', 'data_platform__dates') }}
)

, renamed as (

    select 
        date
        , year as calendar_year
        , year_of_week as year_of_calendar_week
        , quarter as calendar_quarter
        , month_number as calendar_month_number
        , month_name as calendar_month_name
        , week as calendar_week
        , day_of_week
        , weekday_name

    from dates

)

, add_extra_columns as (
    
    select 
        renamed.*
        , {{ get_iso_week_start_date('year_of_calendar_week', 'calendar_week') }} as monday_date
        , date > current_date() as is_future
    from renamed
)

, add_financial_periods as (

    select 
        add_extra_columns.*
        , year(monday_date) as financial_year
        , quarter(monday_date) as financial_quarter
        , month(monday_date) as financial_month_number
        , date_format(monday_date, 'MMMM') as financial_month_name
        , case
            when month(monday_date) = 12 and calendar_week = 1 then 53
            else calendar_week
        end as financial_week

        , monday_date - interval 1 week as monday_date_previous_week

    from add_extra_columns 

)

-- Moving week 40 in 2024. This is how finance want the weeks, since it will then be 13 weeks in each quarter (except Q4 in 2024). 
-- TODO: We might need to do something in year 2030 and the following years
, moving_specific_financial_weeks as (
    
    select 
        date
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

, add_day_indexes as (
    select 
        *
        -- add index to all the days of the financial year
        -- makes it possible to compare with the same day in other years
        -- since some financial years does not have week 1 we cannot use row_number()
        , 7 * (financial_week - 1) + (day_of_week) as day_of_financial_year_number
        -- add index to all the days of each quarter the financial year
        -- makes it possible to compare with the same quarter n periods away
        , row_number() over (partition by financial_year, financial_quarter order by date) as day_of_financial_quarter_number
        -- add index to all the days of each month of the financial year
        -- makes it possible to compare with the same month n periods away
        , row_number() over (partition by financial_year, financial_month_number order by date) as day_of_financial_month_number
    from moving_specific_financial_weeks
)

select * from add_day_indexes