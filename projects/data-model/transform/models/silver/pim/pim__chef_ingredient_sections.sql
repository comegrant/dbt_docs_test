with 

source as (

    select * from {{ source('pim', 'pim__chef_ingredient_sections') }}

),

renamed as (

    select
        {# ids #}
        chef_ingredient_section_id
        , recipe_portion_id

        --{# ints #}
        --, section_order

    from source

)

select * from renamed