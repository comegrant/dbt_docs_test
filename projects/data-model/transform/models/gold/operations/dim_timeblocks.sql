with 

timeblocks_scd1 as (

    select * from {{ ref('operations__timeblocks') }}

)

, timeblock_calendar as (

    select * from {{ ref('operations__timeblock_calendar') }}

)

, timeblock_types as (

    select * from {{ ref('operations__timeblock_types') }}

)
, dates as (

    select * from {{ref('data_platform__dates')}}

)

, weekday_names as (

    select distinct day_of_week, weekday_name from dates

)

-- group consecutive periods of identical timeblocks
, timeblock_calendar_group_consecutive_periods as (

    select
    timeblock_calendar.*
    -- Create group number for consecutive periods of identical timeblocks by finding the row number of the timeblock 
    -- and subtracting the row number of the timeblock with the same day of week, hour from and hour to
    -- Example:
    -- The first group will be 0: 1-1,2-2,3-3 etc.
    -- If the first group has 40 rows, the second group will be 40: 41-1,42-2,43-3 etc.
    , row_number()
            over
            (
                partition by
                    timeblock_id
                order by menu_week_monday_date
            )
        -
    row_number()
            over
            (
                partition by
                    timeblock_id, day_of_week, time_from, time_to
                order by menu_week_monday_date
            )
    as period_group_nr
    from timeblock_calendar

)

-- find valid from and valid to columns for timeblocks based on which group they belong to
, timeblocks_scd2 as (
    select 
        timeblock_id
        , day_of_week
        , time_from
        , time_to
        , time_from_numeric
        , time_to_numeric
        , delivery_window_start_numeric
        , min(menu_week_monday_date) as valid_from
        -- valid to is exclusive, hence we add 7 days to the last menu week monday date where the timeblock is valid
        -- i.e. one join by doing date >= valid_from and date < valid_to
        , cast(date_add(DAY, 7, max(menu_week_monday_date)) as date) as valid_to
        , period_group_nr
    from timeblock_calendar_group_consecutive_periods
    group by all
)

, all_tables_joined as (

    select 
        md5(concat(timeblocks_scd2.timeblock_id, timeblocks_scd2.valid_from)) as pk_dim_timeblocks
        , timeblocks_scd2.timeblock_id
        , timeblocks_scd1.country_id
        , timeblocks_scd1.timeblock_type_id
        , timeblocks_scd2.day_of_week
        , weekday_names.weekday_name as weekday_name
        , concat(timeblocks_scd2.time_from, '-', timeblocks_scd2.time_to) as delivery_time_window_name
        , concat(left(weekday_names.weekday_name, 3), ' ', timeblocks_scd2.time_from, '-', timeblocks_scd2.time_to) as delivery_window_name
        , timeblocks_scd2.delivery_window_start_numeric
        , timeblocks_scd2.time_from
        , timeblocks_scd2.time_to
        , timeblocks_scd2.time_from_numeric
        , timeblocks_scd2.time_to_numeric
        , timeblock_types.timeblock_type_name
        , timeblocks_scd2.valid_from
        , timeblocks_scd2.valid_to
    from timeblocks_scd2
    left join timeblocks_scd1
        on timeblocks_scd2.timeblock_id = timeblocks_scd1.timeblock_id
    left join timeblock_types
        on timeblocks_scd1.timeblock_type_id = timeblock_types.timeblock_type_id
    left join weekday_names
        on timeblocks_scd2.day_of_week = weekday_names.day_of_week

)

select * from all_tables_joined
