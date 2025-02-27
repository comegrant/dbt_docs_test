with 

source as (

    select * from {{ source('cms', 'cms__discount_sub_category') }}

)

, renamed as (

    select

        
        {# ids #}
        id as discount_sub_category_id

        {# strings #}
        , name as discount_sub_category_name
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
