with 

source as (

    select * from {{ source('pim', 'pim__generic_ingredients') }}

)

, renamed as (

    select

        {# ids #}
        -- place ids here
        generic_ingredient_id
        , status_code_id as generic_ingredient_status_code_id

        {# booleans #}
        , nutrition_calculation

        {# system #}
        , created_by     as source_created_by
        , created_date   as source_created_at
        , modified_by    as source_updated_by
        , modified_date  as source_updated_at

        --, unit_label_id
        --, country_id

    from source

)

select * from renamed
