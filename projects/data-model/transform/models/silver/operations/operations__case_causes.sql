with 

source as (

    select * from {{ source('operations', 'operations__case_cause') }}

)

, renamed as (

    select

        {# ids #}
        case_cause_id 
        
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
