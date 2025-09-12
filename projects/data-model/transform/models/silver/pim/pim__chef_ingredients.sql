with 

source as (

    select * from {{ source('pim', 'pim__chef_ingredients') }}

),

renamed as (

    select
        {# ids #}
        chef_ingredient_id
        , chef_ingredient_section_id
        , order_ingredient_id as recipe_ingredient_id
        , generic_ingredient_id

        --{# strings #}
        --, ingredient_amount as recipe_ingredient_amount

        --{# ints #}
        --, ingredient_order

    from source

)

select * from renamed