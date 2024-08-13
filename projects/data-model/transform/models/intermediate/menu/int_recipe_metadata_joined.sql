with

metadata as (

    select * from {{ ref('sil_pim__recipe_metadata') }}

),

metadata_translations as (

    select * from {{ ref('sil_pim__recipe_metadata_translations') }}

),

difficulty_levels_translations as (

    select * from {{ ref('sil_pim__recipe_difficulty_level_translations') }}

),


join_tables as (

select

    metadata.recipe_metadata_id
    , metadata.recipe_main_ingredient_id
    , metadata.recipe_difficulty_level_id
    , metadata_translations.language_id

    , metadata.cooking_time_from
    , metadata.cooking_time_to

    , metadata_translations.recipe_name
    , metadata_translations.recipe_photo_caption
    , metadata_translations.roede_calculation_text
    , metadata_translations.recipe_extra_photo_caption
    , metadata_translations.recipe_general_text
    , metadata_translations.recipe_description
    , difficulty_levels_translations.recipe_difficulty_name
    , difficulty_levels_translations.recipe_difficulty_description


from metadata
left join metadata_translations
on metadata.recipe_metadata_id = metadata_translations.recipe_metadata_id
left join difficulty_levels_translations
on metadata.recipe_difficulty_level_id = difficulty_levels_translations.recipe_difficulty_level_id
and metadata_translations.language_id = difficulty_levels_translations.language_id
    
)

select * from join_tables