with 

source as (

    select * from {{ source('pim', 'pim__recipes_taxonomies') }}

),

renamed as (

    select
        {# ids #}
        recipe_id
        , taxonomies_id as taxonomy_id

    from source

)

select * from renamed