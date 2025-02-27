with

source as (

    select * from {{ source('cms', 'cms__discount_category_mapping_discount_sub_category') }}

)

, renamed as (

    select

        {# ids #}
        discount_category_id
        , discount_sub_category_id

    from source

)

select * from renamed
