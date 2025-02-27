with

source as (

    select * from {{ source('cms', 'cms__discount_mapping_channel_category') }}

)

, renamed as (

    select

        {# ids #}
        discount_channel_id
        , discount_category_id

    from source

)

select * from renamed
