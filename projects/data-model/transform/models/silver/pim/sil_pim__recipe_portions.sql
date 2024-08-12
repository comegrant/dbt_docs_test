with 

source as (

    select * from {{ source('pim', 'pim_recipe_portions') }}

),

renamed as (

    select
        
        {# ids #}
        recipe_portion_id
        , recipe_id
        , portion_id

        {# system #}
        , created_by as system_created_by
        , created_date as system_created_date
        , modified_by as system_modified_by
        , modified_date as system_modified_date

        {# not sure if these are needed
        , recipe_portion_cost
        , organic_pct
        , keyhole
        , chef_partner_score
        #}

    from source

)

select * from renamed