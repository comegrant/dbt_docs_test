with 

source as (

    select * from {{ source('pim', 'pim__nutrient_facts_translations') }}

)

, renamed as (

    select

        {# ids #}
        nutrient_fact_id     as ingredient_nutrient_fact_id
        , language_id

        {# strings #}
        , nutrient_fact_name as ingredient_nutrient_fact_name

    from source

)

select * from renamed
