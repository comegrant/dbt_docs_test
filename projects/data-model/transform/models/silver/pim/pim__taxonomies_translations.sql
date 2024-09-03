with 

source as (

    select * from {{ source('pim', 'pim__taxonomies_translations') }}

),

renamed as (

    select
        {# ids #}
        taxonomies_id as taxonomy_id
        , language_id

        {# strings #}
        , taxonomies_name as taxonomy_name
        --, taxonomies_description as taxonomy_description

    from source

)

select * from renamed