with 

source as (

    select * from {{ source('pim', 'pim_recipe_companies') }}

),

renamed as (

    select

        {# ids #} 
        company_id
        , recipe_id
        
    from source

)

select * from renamed