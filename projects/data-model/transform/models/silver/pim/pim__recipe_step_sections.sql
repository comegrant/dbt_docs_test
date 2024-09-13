with source as (
    select *
    from {{ source('pim', 'pim__recipe_step_sections') }}
),

renamed as (
    select
        recipe_step_section_id,
        recipe_portion_id,
        recipe_step_section_type_id
    from source
)

select * from renamed
