with

preselector_successful_output as (

    select * from {{ ref('int_mloutputs_preselector_successful_realtime_output_explode_product_variations') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

, companies as (

    select * from {{ref('dim_companies')}}

)

, recipes as (

    select * from {{ ref('dim_recipes') }}

)

, generate_keys as (

    select
        -- Primary key
        md5(cast(concat(
            preselector_successful_output.billing_agreement_id,
            preselector_successful_output.output_product_variation_id,
            preselector_successful_output.created_at,
            preselector_successful_output.requested_menu_year,
            preselector_successful_output.requested_menu_week
        ) as string)) as pk_fact_preselector

        -- Timestamps
        , preselector_successful_output.created_at

        -- Identifiers
        , preselector_successful_output.billing_agreement_id
        , preselector_successful_output.company_id
        , preselector_successful_output.output_product_variation_id
        , menus.recipe_id
        , recipes.main_recipe_id
        , preselector_successful_output.model_version_commit_sha

        -- Versioning
        , preselector_successful_output.menu_week_output_version
        , preselector_successful_output.is_latest_menu_week_output_version

        -- Request details
        , preselector_successful_output.requested_menu_year
        , preselector_successful_output.requested_menu_week
        , preselector_successful_output.requested_portions
        , preselector_successful_output.requested_meals

        -- Preferences
        , preselector_successful_output.concept_preference_ids
        , preselector_successful_output.taste_preference_ids
        , preselector_successful_output.taste_preference_compliancy_code

        -- Error metrics
        , preselector_successful_output.error_cooking_time_mean
        , preselector_successful_output.error_is_beef_percentage
        , preselector_successful_output.error_is_chef_choice_percentage
        , preselector_successful_output.error_is_chicken_percentage
        , preselector_successful_output.error_is_cod_percentage
        , preselector_successful_output.error_is_family_friendly_percentage
        , preselector_successful_output.error_is_gluten_free_percentage
        , preselector_successful_output.error_is_grain_percentage
        , preselector_successful_output.error_is_lactose_percentage
        , preselector_successful_output.error_is_lamb_percentage
        , preselector_successful_output.error_is_low_calorie
        , preselector_successful_output.error_is_mixed_meat_percentage
        , preselector_successful_output.error_is_other_carbo_percentage
        , preselector_successful_output.error_is_other_protein_percentage
        , preselector_successful_output.error_is_pasta_percentage
        , preselector_successful_output.error_is_pork_percentage
        , preselector_successful_output.error_is_roede_percentage
        , preselector_successful_output.error_is_salmon_percentage
        , preselector_successful_output.error_is_seafood_percentage
        , preselector_successful_output.error_is_shrimp_percentage
        , preselector_successful_output.error_is_soft_bread_percentage
        , preselector_successful_output.error_is_spicy_percentage
        , preselector_successful_output.error_is_tuna_percentage
        , preselector_successful_output.error_is_vegan_percentage
        , preselector_successful_output.error_is_vegetables_percentage
        , preselector_successful_output.error_is_vegetarian_percentage
        , preselector_successful_output.error_mean_cost_of_food
        , preselector_successful_output.error_mean_energy
        , preselector_successful_output.error_mean_fat
        , preselector_successful_output.error_mean_fat_saturated
        , preselector_successful_output.error_mean_number_of_ratings
        , preselector_successful_output.error_mean_ordered_ago
        , preselector_successful_output.error_mean_protein
        , preselector_successful_output.error_mean_rank
        , preselector_successful_output.error_mean_ratings
        , preselector_successful_output.error_mean_veg_fruit

        -- Foreign keys
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , md5(preselector_successful_output.company_id) as fk_dim_companies
        , md5(concat(preselector_successful_output.output_product_variation_id, preselector_successful_output.company_id)) as fk_dim_products
        , md5(concat(menus.recipe_id, companies.language_id)) as fk_dim_recipes
        , md5(concat(recipes.main_recipe_id, companies.language_id)) as fk_dim_recipes_main_recipe
        , cast(date_format(preselector_successful_output.created_at, 'yyyyMMdd') as int) as fk_dim_date_created_at_preselector_output
        , cast(date_format(preselector_successful_output.created_at, 'HHmm') as int) as fk_dim_time_created_at_preselector_output
        , md5(preselector_successful_output.model_version_commit_sha) as fk_dim_preselector_versions

    from preselector_successful_output
    left join agreements
        on preselector_successful_output.billing_agreement_id = agreements.billing_agreement_id
        and preselector_successful_output.created_at >= agreements.valid_from
        and preselector_successful_output.created_at < agreements.valid_to
    left join companies
        on preselector_successful_output.company_id = companies.company_id
    left join menus
        on preselector_successful_output.requested_menu_year = menus.menu_year
        and preselector_successful_output.requested_menu_week = menus.menu_week
        and preselector_successful_output.output_product_variation_id = menus.product_variation_id
        and preselector_successful_output.company_id = menus.company_id
    left join recipes
        on menus.recipe_id = recipes.recipe_id
        and companies.language_id = recipes.language_id
)

select * from generate_keys
