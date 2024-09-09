with 

source as (

    select * from {{ source('pim', 'pim__recipe_portions') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_portion_id
        , recipe_id
        , portion_id

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        {# not sure if these are needed
        , recipe_portion_cost
        , organic_pct
        , keyhole
        , chef_partner_score
        #}

    from source

)

select * from renamed