with

preselector_successful_output as (

    select * from {{ ref('mloutputs__preselector_successful_realtime_output') }}

)

-- Explode the product_variation_ids array into individual rows
, explode_list as (

    select
        billing_agreement_id,
        company_id,
        concept_preference_ids,
        taste_preference_ids,
        model_version_commit_sha,
        portions,
        meals,
        menu_week,
        menu_year,
        menu_week_monday_date,
        target_cost_of_food_per_meal,
        has_data_processing_consent,
        is_override_deviation,
        product_variation_id,
        -- main_recipe_ids is excluded as it doesn't make sense to include after exploding product_variation_ids
        taste_preference_compliancy_code,
        menu_week_output_version,
        is_most_recent_output,
        created_at,
        preselector_output_id
    from preselector_successful_output
    lateral view explode(product_variation_ids) as product_variation_id

)

select * from explode_list
