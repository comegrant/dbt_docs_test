with 

source as (

    select * from {{ source('pim', 'pim_menu_recipes') }}

),

renamed as (

    select

        {# ids #}
        menu_recipe_id
        , menu_id
        , recipe_id as recipe_id
        
        {# ints #}
        , menu_recipe_order

        {# no clue what this is:
        , menu_recipe_duplicated
        , is_target
        #}

    from source

)

select * from renamed