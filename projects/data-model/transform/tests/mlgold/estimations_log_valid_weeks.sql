-- Makes sure that we generate estimations for all relevant menu_weeks for each company

with filtered_rows as (
    select  *
    from {{ ref('estimations_log') }}
    -- subtract an hour so that this test does not fail when estimations are created just before midnight and this test runs just after midnight
    where date_trunc('day', estimation_generated_at) = date_trunc('day',date_add(hour,-1,current_timestamp))
)

, date_bounds as (
    select
        date_add(current_date, (7 - pmod(datediff(current_date, '1970-01-01'), 7)) + ( 2 * 7)) as start_date,
        date_add(current_date, (7 - pmod(datediff(current_date, '1970-01-01'), 7)) + (11 * 7)) as end_date
)

, expected_menu_weeks as (
    select
        weekofyear(date_sequence) as menu_week
    from (
        select explode(sequence(start_date, end_date, interval 7 days)) as date_sequence
        from date_bounds
    )
)

, expected_companies(company_id) as (
    values
        ('6A2D0B60-84D6-4830-9945-58D518D27AC2') --Linas
        , ('8A613C15-35E4-471F-91CC-972F933331D7') --Adams
        , ('09ECD4F0-AE58-4539-8E8F-9275B1859A19') --Godtlevert
        , ('5E65A955-7B1A-446C-B24F-CFE576BF52D7') --RetNemt
)

, expected_companies_with_weeks as (
    select 
        menu_week
        , company_id
    from expected_menu_weeks 
    left join expected_companies
        on 1=1
)

, missing_companies_with_weeks as (
    select 
        expected_companies_with_weeks.menu_week
        , expected_companies_with_weeks.company_id
    from expected_companies_with_weeks 
    left join filtered_rows
    on 
        expected_companies_with_weeks.menu_week = filtered_rows.menu_week
        and expected_companies_with_weeks.company_id = filtered_rows.company_id
    where filtered_rows.menu_week is null
)

select * from missing_companies_with_weeks