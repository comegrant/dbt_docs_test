with 

source as (

    select * from {{ source('operations', 'operations__case_category') }}

)

, renamed as (

    select
        
        {# ids #}
        case_category_id 
        
        {# strings #}
        , case_category_name as case_category_name
    
    from source

)

select * from renamed
