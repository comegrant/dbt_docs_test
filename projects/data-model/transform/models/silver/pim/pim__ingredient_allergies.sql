with

source as (

    select * from {{ source('pim', 'pim__ingredient_allergies') }}

)

, renamed as (

    select
        {# ids #}
        ingredient_id
        , allergy_id

        {# booleans #}
        , has_trace_of

    from source

)

select * from renamed
