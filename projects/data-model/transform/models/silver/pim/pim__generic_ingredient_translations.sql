with 

source as (

    select * from {{ source('pim', 'pim__generic_ingredients_translations') }}

),

renamed as (

    select
        {# ids #}
        generic_ingredient_id
        , language_id

        {# strings #}
        , generic_ingredient_name
        --, generic_ingredient_description
        --, plural_generic_ingredient_name

    from source

)

select * from renamed