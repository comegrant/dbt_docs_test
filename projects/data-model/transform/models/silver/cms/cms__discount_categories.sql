with 

source as (

    select * from {{ source('cms', 'cms__discount_category') }}

)

, renamed as (

    select


        {# ids #}
        -- place ids here
        category_id as discount_category_id
        {# strings #}
        -- place strings here
        , category_name as discount_category_name
        {# numerics #}
        -- place numerics here

        {# booleans #}
        -- place booleans here

        {# date #}
        -- place dates here

        {# timestamp #}
        -- place timestamps here

        {# scd #}
        -- place slowly change dimension fields here

        {# system #}
        -- place system columns here
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed