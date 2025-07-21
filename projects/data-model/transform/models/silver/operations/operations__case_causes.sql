with 

source as (

    select * from {{ source('operations', 'operations__case_cause') }}

)

, renamed as (

    select

        {# ids #}
        case_cause_id 

        -- generate id to be used in dim case details to fetch case name before 28.01.2025
        -- and responsible description after 28.01.2025 
        , md5(concat_ws('-', 'C', case_cause_id)) as case_cause_responsible_id
        
        {# strings #}
        , case
            when lower(case_cause_name) like '%cancellation%' 
            or lower(case_cause_name) like '%sen avbokning%' 
            or lower(case_cause_name) like '%kansellering%' 
            then 'Cancellation'
            else case_cause_name
            end as case_cause_name
        
    from source

)

select * from renamed