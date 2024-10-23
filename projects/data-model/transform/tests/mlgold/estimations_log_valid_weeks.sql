-- Makes sure that we generate estimations for all relevant menu_weeks

with filtered_rows as (
    select  *
    from {{ ref('estimations_log') }}
    where date_trunc('day', estimation_generated_at) = current_date
)

, date_bounds as (
    select
        date_add(current_date, (7 - pmod(datediff(current_date, '1970-01-01'), 7)) + ( 1 * 7)) as start_date,
        date_add(current_date, (7 - pmod(datediff(current_date, '1970-01-01'), 7)) + (14 * 7)) as end_date
)

, expected_menu_weeks as (
    select
        weekofyear(date_sequence) as menu_week
    from (
        select explode(sequence(start_date, end_date, interval 7 days)) as date_sequence
        from date_bounds
    )
)

, missing_menu_weeks as (
    select expected_menu_weeks.menu_week
    from expected_menu_weeks
    left join filtered_rows
    on expected_menu_weeks.menu_week = filtered_rows.menu_week
    where filtered_rows.menu_week is null
)

select * from missing_menu_weeks