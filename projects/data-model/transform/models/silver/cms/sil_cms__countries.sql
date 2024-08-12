with

source as (
    select * from {{source('cms', 'cms_country')}}
),

renamed as (
    select
        
        {# ids #}
        id as country_id
        ,default_language_id as language_id
        
        {# strings #}
        , name as country_name
        , currency as country_currency

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source
)

select * from renamed