with 

source as (

    select * from {{ source('pim', 'pim__portions_translations') }}

),

renamed as (

    select
        
        {# ids #}
        portion_id
        , language_id

        {# strings #}
        , initcap(portion_name) as portion_name
        
        {# not sure if these are needed
        , portion_description
        #}

    from source

)

select * from renamed