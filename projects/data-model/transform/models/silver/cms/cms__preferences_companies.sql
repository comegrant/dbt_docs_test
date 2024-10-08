with 

source as (

    select * from {{ source('cms', 'cms__preference_company') }}

)

, renamed as (

    select

        
        {# ids #}
        upper(preference_id) as preference_id
        , company_id

        {# strings #}
        , name as preference_name
        , description as preference_description
        
        {# booleans #}
        , is_active as is_active_preference
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
