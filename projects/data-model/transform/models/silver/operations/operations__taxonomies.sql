with 

source as (

    select * from {{ source('operations', 'operations__taxonomies') }}

)

, renamed as (

    select

        {# ids #}
        taxonomy_id 
        , country_id

        {# strings #}
        , taxonomy_name_shown as taxonomy_name
        --, taxonomy_name is the same as taxonomy_name, so we can remove it
        
        {# booleans #}
        , is_active as is_active_taxonomy

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
