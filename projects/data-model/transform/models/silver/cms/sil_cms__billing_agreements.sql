with 

source as (

    select * from {{ source('cms', 'cms_billing_agreement') }}

),

renamed as (

    select
        
        {# ids #}
        agreement_id
        , customer_id
        , company_id
        , status as billing_agreement_status_id
        , sales_point_id
        --, payment_partner_id
        --, applicant_id
        --, trigger_registration_process as trigger_registration_process_id
        
        {# strings #}
        , initcap(method_code) as payment_method
        , initcap(source) as signup_source
        , upper(sales_person) as signup_salesperson

        {# booleans #}
        --, addresses_copied

        {# date #}
        , to_date(created_at) as signup_date
        , extract('DOY', created_at) as signup_isoyear_day
        , extract('DAY', created_at) as signup_month_day
        , extract('DAYOFWEEK_ISO', created_at) as signup_isoweek_day
        , extract('WEEK', created_at) as signup_isoweek
        , extract('MONTH', created_at) as signup_month
        , extract('QUARTER', created_at) as signup_quarter
        , extract('YEAROFWEEK', created_at) as signup_isoyear

        , to_date(start_date) as start_date
        , extract('DOY', start_date) as start_isoyear_day
        , extract('DAY', start_date) as start_month_day
        , extract('DAYOFWEEK_ISO', start_date) as start_isoweek_day
        , extract('WEEK', start_date) as start_isoweek
        , extract('MONTH', start_date) as start_month
        , extract('QUARTER', start_date) as start_quarter
        , extract('YEAROFWEEK', start_date) as start_isoyear

        {# timestamp #}
        , start_date as start_at

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by


    from source

)

select * from renamed