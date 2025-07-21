with 

source as (

    select * from {{ source('operations', 'operations__case_responsible') }}

)

, renamed as (

    select
    
        {# ids #}
        case_responsible_id

        -- generate id to be used in dim case details to fetch case name before 28.01.2025
        -- and responsible description after 28.01.2025 
        , md5(concat_ws('-', 'R', case_responsible_id)) as case_cause_responsible_id
        
        {# strings #}
        , case_responsible_name as case_responsible_description
        , case 
            when lower(case_responsible_name) like '%internal%' then 'Internal' 
            when lower(case_responsible_name) like '%external%' then 'External' 
            else 'Other' 
            end as case_responsibility_type
    from source
)

select * from renamed