with 

source as (

    select * from {{ source('operations', 'operations__case_line_type') }}

)

, renamed as (

    select

        {# ids #}
        id as case_line_type_id 
        
        {# strings #}
        , initcap(description) as case_line_type_name
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        
        

    from source
)

select * from renamed
