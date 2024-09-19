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
        , ingredient_category_name
        --, ingredient_category_description
        --, frontend_display_name

    from source

)

select * from renamed