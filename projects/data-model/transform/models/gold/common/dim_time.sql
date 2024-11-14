with 

hours as (
    select explode(sequence(0, 23)) as hour
)

, minutes as (
    select explode(sequence(0, 59)) as minute
)

, hours_and_minutes_joined as (
    select
        cast(
            concat(
                format_string('%02d', hours.hour),
                format_string('%02d', minutes.minute)
                ) as string 
        ) as pk_dim_time
        , hours.hour
        , minutes.minute
    from hours
    cross join minutes
)

select * from hours_and_minutes_joined