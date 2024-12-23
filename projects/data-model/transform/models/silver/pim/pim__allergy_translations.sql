with

source as (

    select * from {{ source('pim', 'pim__allergies_translations') }}

)

, renamed as (

    select
        {# ids #}
        allergy_id
        , language_id

        {# strings #}
        , lower(allergy_name) as allergy_name

    from source

)

select * from renamed
