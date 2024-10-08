with 

source as (

    select * from {{ source('cms', 'cms__preference_attribute_value') }}

)

, renamed as (

    select

        
        {# ids #}
        attribute_id
        , upper(preference_id) as preference_id
        , company_id

        {# strings #}
        , upper(attribute_value) as attribute_value
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
