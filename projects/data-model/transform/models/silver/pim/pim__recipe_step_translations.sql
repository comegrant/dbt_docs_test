with

source as (

    select * from {{ source('pim', 'pim__recipe_steps_translations') }}

)

, renamed as (

    select
        {# ids #}
        recipe_step_id
        , language_id

        {# strings #}
        , recipe_step_description

    from source

)

select * from renamed
