with

source as (

    select * from {{ source('pim', 'pim__taxonomy_types') }}

),

renamed as (

    select
        {# ids #}
        id as taxonomy_type_id

        {# strings #}
        , lower(name) as taxonomy_type_name


    from source

)

select * from renamed
