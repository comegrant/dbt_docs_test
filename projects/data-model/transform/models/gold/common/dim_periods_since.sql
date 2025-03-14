with

days_array as (
    select sequence(0, 50000) as days_array
)

, days_exploded as (
    select explode(days_array) as day
    from days_array
)

, add_other_periods as
(
    select

    cast(day as int) as pk_dim_periods_since
    , cast(day as int) as days_since
    , cast(floor(day / 7) as int) as weeks_since
    , cast(floor(day / 30) as int) as months_since
    , cast(floor(day / 365 * 4) as int) as quarters_since
    , cast(floor(day / 365) as int) as years_since

    from days_exploded
)

select * from add_other_periods
