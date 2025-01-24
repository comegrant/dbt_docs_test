with 

source as (

    select * from {{ ref('scd_pim__recipe_favorites') }}

)

, renamed as (

    select

        
        {# ids #}
        recipe_favorite_id
        , agreement_id as billing_agreement_id
        , recipe_id
        , main_recipe_id
        , recipe_favorite_type_id

        {# boolean #}
        , dbt_valid_to is null as is_active_reaction

        {# scd #}
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by

    from source

)

select * from renamed
