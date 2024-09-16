with 

source as (

    select * from {{ source('cms', 'cms__delivery_week_type') }}

)

, renamed as (

    select
        
        {# ids #}
        week_type_id as delivery_week_type_id

        {# strings #}
        , week_type_name as delivery_week_type_name

    from source

)

select * from renamed
