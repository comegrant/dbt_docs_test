with 

source as (

    select * from {{ source('pim', 'pim__recipes_translations') }}

),

renamed as (

    select
        
        {# ids #}
          recipe_id
        , language_id
        
        {# strings #}
        , recipe_comment
        , recipe_chef_tip

        {# not sure if these are needed
        , menu_planning_comment
        #}

    from source

)

select * from renamed