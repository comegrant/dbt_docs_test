with 

source as (

    select * from {{ source('operations', 'operations__cutoff') }}

)

, renamed as (

    select

        
        {# ids #}
        id as cutoff_id
        , country_id

        {# strings #}
        , name as cutoff_name
        , description as cutoff_description
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
