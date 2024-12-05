-- The timestamps in this table has historically been local time. 
-- However on 2024-11-06 they updated the system registration system to use UTC. 
-- Hence we will only convert to UTC before this change.
{% set UTC_start_time = '2024-11-06 16:30:00' %}

with 

source as (

    select * from {{ ref('scd_cms__billing_agreements') }}

),

renamed as (

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
        , to_date(created_at) as signup_date
        , extract('DOY', created_at) as signup_year_day
        , extract('DAY', created_at) as signup_month_day
        , extract('DAYOFWEEK_ISO', created_at) as signup_week_day
        , extract('WEEK', created_at) as signup_week
        , extract('MONTH', created_at) as signup_month
        , extract('QUARTER', created_at) as signup_quarter
        , extract('YEAROFWEEK', created_at) as signup_year

        , to_date(start_date) as start_date
        , extract('DOY', start_date) as start_year_day
        , extract('DAY', start_date) as start_month_day
        , extract('DAYOFWEEK_ISO', start_date) as start_week_day
        , extract('WEEK', start_date) as start_week
        , extract('MONTH', start_date) as start_month
        , extract('QUARTER', start_date) as start_quarter
        , extract('YEAROFWEEK', start_date) as start_year

        {# timestamp #}
        , case
            when created_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', created_at)
            else created_at
        end as signup_at

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
        , case
            when created_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', created_at)
            else created_at
        end as source_created_at

        , created_by as source_created_by

        , case
            when updated_at < '{{ UTC_start_time }}' then convert_timezone('Europe/Oslo', 'UTC', updated_at)
            else updated_at
        end as source_updated_at

        , updated_by as source_updated_by


    from source

)

select * from renamed