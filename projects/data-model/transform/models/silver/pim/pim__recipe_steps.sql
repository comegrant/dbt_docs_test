with source as (
    select
        *
    from {{ source('pim', 'pim__recipe_steps') }}
),

renamed_and_filtered as (
    select
        {# id #}
        recipe_step_id,
        recipe_step_section_id,

        {# numerical column #}
        recipe_step_order

        {# recipe_step_timer column not kept due to too many NULLs #}
    from source
    where recipe_step_section_id is not null
)

select * from renamed_and_filtered
