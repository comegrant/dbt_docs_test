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
        -- Removing OneSub: prefix from preference names
        , case
            when name like 'OneSub:%' then initcap(trim(substring(name, 8)))
            else initcap(name)
          end as preference_name
        , case
            when name like 'OneSub:%' then true
            else false
          end as is_onesub_concept_preference
        , description as preference_description

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
