with 

source as (

    select * from {{ source('operations', 'operations__case_taxonomies') }}

)

, renamed as (

    select

        
        {# ids #}
        -- place ids here
        case_id
        , taxonomy_id 
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
