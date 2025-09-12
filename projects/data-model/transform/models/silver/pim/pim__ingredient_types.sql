with 

source as (

    select * from {{ source('pim', 'pim__ingredient_type') }}

),

renamed as (

    select

        {# ids #}
        id as ingredient_type_id

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

        {# strings #}
        , name as ingredient_type_name

    from source

)

select * from renamed