with 

source as (

    select * from {{ source('cms', 'cms__preference') }}

)

, renamed as (

    select

        
        {# ids #}
        upper(preference_id) as preference_id
        , preference_type_id

        {# strings #}
        , name as preference_name_general
        , description as preference_description_general

        {# numerics #}
        -- place numerics here
        
        {# booleans #}
        , is_allergen
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
