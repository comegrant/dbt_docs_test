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
        , upper(substring(ingredient_name, 1, 1)) || lower(substring(ingredient_name, 2)) as ingredient_name

    from source

)

select * from renamed
