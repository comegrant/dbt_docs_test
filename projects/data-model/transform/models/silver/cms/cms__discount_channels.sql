with

source as (

    select * from {{ source('cms', 'cms__discount_channel') }}

)

, renamed as (

    select


    {# ids #}
        -- place ids here
        id           as discount_channel_id

        {# strings #}
        -- place strings here
        , name       as discount_channel_name

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
