with 

source as (

    select * from {{ source('operations', 'operations__case_cause') }}

)

, renamed as (

    select

        {# ids #}
        case_cause_id 
        
        {# strings #}
        , case_cause_name as case_cause_name
        
    from source

)

select * from renamed
