with dim_recipes as (
    select distinct
        main_recipe_id,
        recipe_metadata_id,
        has_recipe_photo
    from prod.gold.dim_recipes
),

recipe_photos as (
    select
        recipe_metadata_id,
        recipe_photo
    from prod.silver.pim__recipe_metadata
)

select
    dim_recipes.*,
    recipe_photo
from
    dim_recipes
left join
    recipe_photos
on recipe_photos.recipe_metadata_id = dim_recipes.recipe_metadata_id
where has_recipe_photo = true
