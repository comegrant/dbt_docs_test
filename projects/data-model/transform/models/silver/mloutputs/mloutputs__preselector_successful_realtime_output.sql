with preselector_successful_output as (

    select * from {{ source('mloutputs', 'preselector_successful_realtime_output') }}

)

, renamed as (

    select
        billing_agreement_id,
        company_id,
        concept_preference_ids,
        taste_preferences as taste_preference_ids,
        model_version,
        portion_size as requested_portions,
        number_of_recipes as requested_meals,
        menu_week as requested_menu_week,
        menu_year as requested_menu_year,
        target_cost_of_food_per_recipe as requested_cost_of_food_per_meal,
        has_data_processing_consent,
        override_deviation as is_override_deviation,
        variation_ids as output_product_variation_ids,
        main_recipe_ids as output_main_recipe_ids,
        compliancy as taste_preference_compliancy_code,
        --error_vector,
        error_vector.cooking_time_mean as error_cooking_time_mean,
        error_vector.is_beef_percentage as error_is_beef_percentage,
        error_vector.is_chef_choice_percentage as error_is_chef_choice_percentage,
        error_vector.is_chicken_percentage as error_is_chicken_percentage,
        error_vector.is_cod_percentage as error_is_cod_percentage,
        error_vector.is_family_friendly_percentage as error_is_family_friendly_percentage,
        error_vector.is_gluten_free_percentage as error_is_gluten_free_percentage,
        error_vector.is_grain_percentage as error_is_grain_percentage,
        error_vector.is_lactose_percentage as error_is_lactose_percentage,
        error_vector.is_lamb_percentage as error_is_lamb_percentage,
        error_vector.is_low_calorie as error_is_low_calorie,
        error_vector.is_mixed_meat_percentage as error_is_mixed_meat_percentage,
        error_vector.is_other_carbo_percentage as error_is_other_carbo_percentage,
        error_vector.is_other_protein_percentage as error_is_other_protein_percentage,
        error_vector.is_pasta_percentage as error_is_pasta_percentage,
        error_vector.is_pork_percentage as error_is_pork_percentage,
        error_vector.is_roede_percentage as error_is_roede_percentage,
        error_vector.is_salmon_percentage as error_is_salmon_percentage,
        error_vector.is_seafood_percentage as error_is_seafood_percentage,
        error_vector.is_shrimp_percentage as error_is_shrimp_percentage,
        error_vector.is_soft_bread_percentage as error_is_soft_bread_percentage,
        error_vector.is_spicy_percentage as error_is_spicy_percentage,
        error_vector.is_tuna_percentage as error_is_tuna_percentage,
        error_vector.is_vegan_percentage as error_is_vegan_percentage,
        error_vector.is_vegetables_percentage as error_is_vegetables_percentage,
        error_vector.is_vegetarian_percentage as error_is_vegetarian_percentage,
        error_vector.mean_cost_of_food as error_mean_cost_of_food,
        error_vector.mean_energy as error_mean_energy,
        error_vector.mean_fat as error_mean_fat,
        error_vector.mean_fat_saturated as error_mean_fat_saturated,
        error_vector.mean_number_of_ratings as error_mean_number_of_ratings,
        error_vector.mean_ordered_ago as error_mean_ordered_ago,
        error_vector.mean_protein as error_mean_protein,
        error_vector.mean_rank as error_mean_rank,
        error_vector.mean_ratings as error_mean_ratings,
        error_vector.mean_veg_fruit as error_mean_veg_fruit,
        generated_at as created_at
    from preselector_successful_output
)

select * from renamed
