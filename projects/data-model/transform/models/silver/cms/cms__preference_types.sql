with 

source as (

    select * from {{ source('cms', 'cms__preference_type') }}

)

, renamed as (

    select

        
        {# ids #}
        preference_type_id

        {# strings #}
        , preference_type_name
        , preference_type_description
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
