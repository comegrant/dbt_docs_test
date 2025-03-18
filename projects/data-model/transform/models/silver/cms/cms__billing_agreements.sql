-- The timestamps in this table has historically been local time. 
-- However on 2024-11-06 they updated the system registration system to use UTC. 
-- Hence we will only convert to UTC before this change.
{% set UTC_start_time = '2024-11-06 16:30:00' %}

with 

source as (

    select * from {{ ref('scd_cms__billing_agreements') }}

)

-- The created_at timestamp of the billing_agreement table is used to determine the signup timestamp of the customer. 
-- However for older customers this will be incorrect. 
-- Hence, if the date of the created_at-timestamp is greater than the start_date of the customer we use the start_date to determine when the customer signed_up. 
, determine_signup_at as (

    select 
        *
        , case when 
        date(start_date) < date(created_at) 
        then start_date
        else created_at end as signup_at
    from source

)

-- Adjusting all timestamps to UTC where the time is not 00:00:00
, convert_timezones as (

    select 
        *
        , case 
            when date_format(created_at, 'HH:mm:ss') = '00:00:00' then created_at
            when created_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', created_at) 
            else created_at 
            end as created_at_corrected
        , case 
            when updated_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', updated_at) 
            else updated_at 
            end as updated_at_corrected
        , case 
            when date_format(start_date, 'HH:mm:ss') = '00:00:00' then start_date
            when start_date < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', start_date) 
            else start_date 
            end as start_date_corrected
        , case 
            when date_format(signup_at, 'HH:mm:ss') = '00:00:00' then signup_at
            when signup_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', signup_at) 
            else signup_at
            end as signup_at_corrected
    from determine_signup_at

)

, renamed as (

    select
        
        {# ids #}
        agreement_id as billing_agreement_id
        --, customer_id
        , company_id
        , status as billing_agreement_status_id
        , sales_point_id
        --, payment_partner_id
        --, applicant_id
        --, trigger_registration_process as trigger_registration_process_id
        
        {# strings #}
        , upper(trim(method_code)) as payment_method
        , upper(trim(source)) as signup_source
        , upper(sales_person) as signup_salesperson

        {# booleans #}
        --, addresses_copied

        {# date #}
        , to_date(signup_at_corrected) as signup_date
        , extract('DOY', signup_at_corrected) as signup_year_day
        , extract('DAY', signup_at_corrected) as signup_month_day
        , extract('DAYOFWEEK_ISO', signup_at_corrected) as signup_week_day
        , extract('WEEK', signup_at_corrected) as signup_week
        , date_format(signup_at_corrected, 'MMMM') as signup_month
        , extract('QUARTER', signup_at_corrected) as signup_quarter
        , extract('YEAROFWEEK', signup_at_corrected) as signup_year

        , to_date(start_date_corrected) as start_date
        , extract('DOY', start_date_corrected) as start_year_day
        , extract('DAY', start_date_corrected) as start_month_day
        , extract('DAYOFWEEK_ISO', start_date_corrected) as start_week_day
        , extract('WEEK', start_date_corrected) as start_week
        , extract('MONTH', start_date_corrected) as start_month
        , extract('QUARTER', start_date_corrected) as start_quarter
        , extract('YEAROFWEEK', start_date_corrected) as start_year

        {# timestamp #}
        , signup_at_corrected as signup_at

        {# scd #}
        , case
            when dbt_valid_from < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', dbt_valid_from)
            else dbt_valid_from
        end as valid_from

        , case
            when dbt_valid_to is null then {{ get_scd_valid_to() }}
            when dbt_valid_to < '{{ UTC_start_time }}'  then convert_timezone('Europe/Oslo', 'UTC', dbt_valid_to)
            else dbt_valid_to
        end as valid_to

        {# system #}
        , created_at_corrected as source_created_at

        , created_by as source_created_by

        , updated_at_corrected as source_updated_at

        , updated_by as source_updated_by


    from convert_timezones

)

select * from renamed