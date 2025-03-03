with 

source as (

    select * from {{ source('pim', 'pim__ingredient_nutrient_facts') }}

)

, renamed as (

    select
        {# ids #}
        nutrient_fact_id as ingredient_nutrient_fact_id
        , ingredient_id

        {# numerics #}
        , nutritional_value as ingredient_nutritional_value
        
        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

select * from renamed
