with

preselector_successful_output_with_output_versions as (

    select * from {{ ref('int_mloutputs_preselector_successful_realtime_output_add_output_versions') }}
)

, explode_list as (

    select
        billing_agreement_id,
        company_id,
        concept_preference_ids,
        taste_preference_ids,
        model_version_commit_sha,
        requested_portions,
        requested_meals,
        requested_menu_week,
        requested_menu_year,
        requested_cost_of_food_per_meal,
        has_data_processing_consent,
        is_override_deviation,
        output_product_variation_id,
        output_main_recipe_ids,
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
        menu_week_output_version,
        is_latest_menu_week_output_version,
        created_at
    from preselector_successful_output_with_output_versions
    lateral view explode(output_product_variation_ids) as output_product_variation_id

)

select * from explode_list
