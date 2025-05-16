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

    from source
)

select * from renamed
