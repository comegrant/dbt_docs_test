with 

source as (

    select * from {{ source('pim', 'pim__ingredient_categories_translations') }}

),

renamed as (

    select
        {# ids #}
        ingredient_category_id
        , language_id

        {# strings #}
        , initcap(ingredient_category_name) as ingredient_category_name
        , initcap(ingredient_category_description) as ingredient_category_description
        --, frontend_display_name

    from source

)

select * from renamed
