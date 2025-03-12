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
        error_cooking_time_mean,
        error_is_beef_percentage,
        error_is_chef_choice_percentage,
        error_is_chicken_percentage,
        error_is_cod_percentage,
        error_is_family_friendly_percentage,
        error_is_gluten_free_percentage,
        error_is_grain_percentage,
        error_is_lactose_percentage,
        error_is_lamb_percentage,
        error_is_low_calorie,
        error_is_mixed_meat_percentage,
        error_is_other_carbo_percentage,
        error_is_other_protein_percentage,
        error_is_pasta_percentage,
        error_is_pork_percentage,
        error_is_roede_percentage,
        error_is_salmon_percentage,
        error_is_seafood_percentage,
        error_is_shrimp_percentage,
        error_is_soft_bread_percentage,
        error_is_spicy_percentage,
        error_is_tuna_percentage,
        error_is_vegan_percentage,
        error_is_vegetables_percentage,
        error_is_vegetarian_percentage,
        error_mean_cost_of_food,
        error_mean_energy,
        error_mean_fat,
        error_mean_fat_saturated,
        error_mean_number_of_ratings,
        error_mean_ordered_ago,
        error_mean_protein,
        error_mean_rank,
        error_mean_ratings,
        error_mean_veg_fruit,
        error_repeated_proteins_percentage,
        error_repeated_carbohydrates_percentage,
        menu_week_output_version,
        is_most_recent_output,
        created_at,
        preselector_output_id
    from preselector_successful_output
    lateral view explode(product_variation_ids) as product_variation_id

)

select * from explode_list
