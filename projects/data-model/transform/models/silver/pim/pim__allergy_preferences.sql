with

source as (

    select * from {{ source('pim', 'pim__allergies_preference') }}

)

, renamed as (

    select
        {# ids #}
        allergy_id
        , preference_id

    from source

)

select * from renamed
