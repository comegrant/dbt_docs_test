with 

source as (

    select * from {{ source('pim', 'pim__recipe_favorite_types') }}

)

, renamed as (

    select

        
        {# ids #}
        recipe_favorite_type_id
        , initcap(recipe_favorite_type_name) as recipe_favorite_type_name
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
