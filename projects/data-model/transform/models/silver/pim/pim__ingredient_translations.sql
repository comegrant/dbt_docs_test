with 

source as (

    select * from {{ source('pim', 'pim__ingredients_translations') }}

)

, renamed as (

    select

        {# ids #}
        ingredient_id
        , language_id

        {# strings #}
        , lower(ingredient_name) as ingredient_name

    from source

)

select * from renamed
