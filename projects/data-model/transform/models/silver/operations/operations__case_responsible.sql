with 

source as (

    select * from {{ source('operations', 'operations__case_responsible') }}

)

, renamed as (

    select
    
        {# ids #}
        case_responsible_id 
        
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
