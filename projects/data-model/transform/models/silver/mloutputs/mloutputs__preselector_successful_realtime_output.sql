with preselector_successful_output as (

    select * from {{ source('prod__mloutputs', 'preselector_successful_realtime_output') }}

)

, add_output_version as (
    select *
    , row_number() over (
        partition by billing_agreement_id, menu_week, menu_year
        order by generated_at asc
    ) as menu_week_output_version
    , case
    when 1 = row_number() over (
        partition by billing_agreement_id, menu_week, menu_year
        order by generated_at desc
    ) then 1
    else 0
    end as is_latest_menu_week_output_version
    from preselector_successful_output
)

, renamed as (

    select
        billing_agreement_id,
        company_id,
        concept_preference_ids,
        taste_preference_ids,
        model_version as model_version_commit_sha,
        portion_size as portions,
        number_of_recipes as meals,
        menu_week,
        menu_year,
        {{ get_iso_week_start_date('menu_year', 'menu_week') }} as menu_week_monday_date,
        target_cost_of_food_per_recipe as target_cost_of_food_per_meal,
        has_data_processing_consent,
        override_deviation as is_override_deviation,
        variation_ids as product_variation_ids,
        main_recipe_ids,
        compliancy as taste_preference_compliancy_code,
        coalesce(error_vector.cooking_time_mean, 0) as error_cooking_time_mean,
        coalesce(error_vector.is_beef_percentage, 0) as error_is_beef_percentage,
        coalesce(error_vector.is_chef_choice_percentage, 0) as error_is_chef_choice_percentage,
        coalesce(error_vector.is_chicken_percentage, 0) as error_is_chicken_percentage,
        coalesce(error_vector.is_cod_percentage, 0) as error_is_cod_percentage,
        coalesce(error_vector.is_family_friendly_percentage, 0) as error_is_family_friendly_percentage,
        coalesce(error_vector.is_gluten_free_percentage, 0) as error_is_gluten_free_percentage,
        coalesce(error_vector.is_grain_percentage, 0) as error_is_grain_percentage,
        coalesce(error_vector.is_lactose_percentage, 0) as error_is_lactose_percentage,
        coalesce(error_vector.is_lamb_percentage, 0) as error_is_lamb_percentage,
        coalesce(error_vector.is_low_calorie, 0) as error_is_low_calorie,
        coalesce(error_vector.is_mixed_meat_percentage, 0) as error_is_mixed_meat_percentage,
        coalesce(error_vector.is_other_carbo_percentage, 0) as error_is_other_carbo_percentage,
        coalesce(error_vector.is_other_protein_percentage, 0) as error_is_other_protein_percentage,
        coalesce(error_vector.is_pasta_percentage, 0) as error_is_pasta_percentage,
        coalesce(error_vector.is_pork_percentage, 0) as error_is_pork_percentage,
        coalesce(error_vector.is_roede_percentage, 0) as error_is_roede_percentage,
        coalesce(error_vector.is_salmon_percentage, 0) as error_is_salmon_percentage,
        coalesce(error_vector.is_seafood_percentage, 0) as error_is_seafood_percentage,
        coalesce(error_vector.is_shrimp_percentage, 0) as error_is_shrimp_percentage,
        coalesce(error_vector.is_soft_bread_percentage, 0) as error_is_soft_bread_percentage,
        coalesce(error_vector.is_spicy_percentage, 0) as error_is_spicy_percentage,
        coalesce(error_vector.is_tuna_percentage, 0) as error_is_tuna_percentage,
        coalesce(error_vector.is_vegan_percentage, 0) as error_is_vegan_percentage,
        coalesce(error_vector.is_vegetables_percentage, 0) as error_is_vegetables_percentage,
        coalesce(error_vector.is_vegetarian_percentage, 0) as error_is_vegetarian_percentage,
        coalesce(error_vector.mean_cost_of_food, 0) as error_mean_cost_of_food,
        coalesce(error_vector.mean_energy, 0) as error_mean_energy,
        coalesce(error_vector.mean_fat, 0) as error_mean_fat,
        coalesce(error_vector.mean_fat_saturated, 0) as error_mean_fat_saturated,
        coalesce(error_vector.mean_number_of_ratings, 0) as error_mean_number_of_ratings,
        coalesce(error_vector.mean_ordered_ago, 0) as error_mean_ordered_ago,
        coalesce(error_vector.mean_protein, 0) as error_mean_protein,
        coalesce(error_vector.mean_rank, 0) as error_mean_rank,
        coalesce(error_vector.mean_ratings, 0) as error_mean_ratings,
        coalesce(error_vector.mean_veg_fruit, 0) as error_mean_veg_fruit,
        coalesce(error_vector.repeated_proteins_percentage, 0) as error_repeated_proteins_percentage,
        coalesce(error_vector.repeated_carbo_percentage, 0) as error_repeated_carbohydrates_percentage,
        menu_week_output_version,
        is_latest_menu_week_output_version,
        generated_at as created_at
    from add_output_version

)

-- Deplucating due to bug in the output, see issue data-536
, deduplicated as (
    select distinct
        billing_agreement_id
        , company_id
        , menu_week
        , menu_year
        , menu_week_monday_date
        , model_version_commit_sha
        , concept_preference_ids
        , taste_preference_ids
        , product_variation_ids
        , main_recipe_ids
        , portions
        , meals
        , target_cost_of_food_per_meal
        , has_data_processing_consent
        , is_override_deviation
        , taste_preference_compliancy_code
        , error_cooking_time_mean
        , error_is_beef_percentage
        , error_is_chef_choice_percentage
        , error_is_chicken_percentage
        , error_is_cod_percentage
        , error_is_family_friendly_percentage
        , error_is_gluten_free_percentage
        , error_is_grain_percentage
        , error_is_lactose_percentage
        , error_is_lamb_percentage
        , error_is_low_calorie
        , error_is_mixed_meat_percentage
        , error_is_other_carbo_percentage
        , error_is_other_protein_percentage
        , error_is_pasta_percentage
        , error_is_pork_percentage
        , error_is_roede_percentage
        , error_is_salmon_percentage
        , error_is_seafood_percentage
        , error_is_shrimp_percentage
        , error_is_soft_bread_percentage
        , error_is_spicy_percentage
        , error_is_tuna_percentage
        , error_is_vegan_percentage
        , error_is_vegetables_percentage
        , error_is_vegetarian_percentage
        , error_mean_cost_of_food
        , error_mean_energy
        , error_mean_fat
        , error_mean_fat_saturated
        , error_mean_number_of_ratings
        , error_mean_ordered_ago
        , error_mean_protein
        , error_mean_rank
        , error_mean_ratings
        , error_mean_veg_fruit
        , error_repeated_proteins_percentage
        , error_repeated_carbohydrates_percentage
        , menu_week_output_version
        , is_latest_menu_week_output_version
        , created_at
    from renamed
)

select * from deduplicated
