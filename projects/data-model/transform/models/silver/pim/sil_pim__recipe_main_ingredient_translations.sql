with 

source as (

    select * from {{ source('pim', 'pim_recipe_main_ingredients_translations') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_main_ingredient_id
        , language_id

        {# strings #}
        , initcap(recipe_main_ingredient_name) as recipe_main_ingredient_name
        , initcap(recipe_main_ingredient_description) as recipe_main_ingredient_description
        
    from source

)

select * from renamed