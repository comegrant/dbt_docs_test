with preselector_batch_output as (

    select * from {{ source('prod__mloutputs', 'preselector_batch') }}

)

, rename as (

    select
        agreement_id as billing_agreement_id
        , company_id
        , portion_size as portions
        , size(recipes) as meals
        , week as menu_week
        , year as menu_year
        , {{ get_iso_week_start_date('year', 'week') }} as menu_week_monday_date
        , recipes.variation_id as product_variation_ids
        , recipes.main_recipe_id as main_recipe_ids
        , compliancy as mealkit_taste_preference_compliancy_code
        -- The dish compliancy code list indexes match with the corresponding index in the recipe or product variation id lists
        , recipes.compliancy as dish_taste_preference_compliancy_codes
        , concept_preference_ids
        , taste_preferences as taste_preference_ids
        , model_version as model_version_commit_sha
        , generated_at as created_at
    from preselector_batch_output

)

select * from rename
