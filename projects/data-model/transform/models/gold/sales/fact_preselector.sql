with

preselector_successful_output as (

    select * from {{ ref('int_mloutputs_preselector_successful_realtime_output_explode_product_variations') }}

)

, deviations as (

    select * from {{ ref('int_basket_deviation_products_joined_versions') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, companies as (

    select * from {{ ref('dim_companies') }}

)

, recipes as (

    select * from {{ ref('dim_recipes') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, preselector_output_generate_keys as (

    select
        -- Primary key
        md5(
            cast(concat(
                preselector_successful_output.billing_agreement_id
                , preselector_successful_output.product_variation_id
                , preselector_successful_output.created_at
                , preselector_successful_output.menu_year
                , preselector_successful_output.menu_week
            ) as string)
        )                                      as pk_fact_preselector

        -- Timestamps
        , preselector_successful_output.created_at

        -- Identifiers
        , preselector_successful_output.billing_agreement_id
        , preselector_successful_output.company_id
        , preselector_successful_output.product_variation_id
        , menus.recipe_id
        , recipes.main_recipe_id
        , preselector_successful_output.model_version_commit_sha

        -- Versioning
        , preselector_successful_output.menu_week_output_version
        , preselector_successful_output.is_most_recent_output

        -- Request details
        , preselector_successful_output.menu_year
        , preselector_successful_output.menu_week
        , preselector_successful_output.menu_week_monday_date
        , preselector_successful_output.portions
        , preselector_successful_output.meals

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
        , preselector_successful_output.error_repeated_proteins_percentage
        , preselector_successful_output.error_repeated_carbohydrates_percentage

        -- Foreign keys
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , md5(
            preselector_successful_output.company_id
        )                                      as fk_dim_companies
        , md5(
            concat(
                preselector_successful_output.product_variation_id
                , preselector_successful_output.company_id
            )
        )                                      as fk_dim_products
        , md5(
            concat(menus.recipe_id, companies.language_id)
        )                                      as fk_dim_recipes
        , cast(
            date_format(preselector_successful_output.created_at, 'yyyyMMdd') as int
        )                                      as fk_dim_dates
        , cast(
            date_format(preselector_successful_output.created_at, 'HHmm') as int
        )                                      as fk_dim_time
        , md5(
            preselector_successful_output.model_version_commit_sha
        )                                      as fk_dim_preselector_versions

    from preselector_successful_output
    left join agreements
        on
            preselector_successful_output.billing_agreement_id = agreements.billing_agreement_id
            and preselector_successful_output.created_at >= agreements.valid_from
            and preselector_successful_output.created_at < agreements.valid_to
    left join companies
        on preselector_successful_output.company_id = companies.company_id
    left join menus
        on
            preselector_successful_output.menu_year = menus.menu_year
            and preselector_successful_output.menu_week = menus.menu_week
            and preselector_successful_output.product_variation_id = menus.product_variation_id
            and preselector_successful_output.company_id = menus.company_id
    left join recipes
        on
            menus.recipe_id = recipes.recipe_id
            and companies.language_id = recipes.language_id
    -- Only select recipes that are selected for the menu, as we are joining in future menu weeks where the recipes are not finalised yet
    where menus.is_selected_menu = true
    and menus.recipe_id is not null
)

-- TODO: add this as an intermediate table
, join_deviations_and_recipes as (

    select distinct
        deviations.menu_week_monday_date
        , deviations.menu_week
        , deviations.menu_year
        , deviations.billing_agreement_id
        , deviations.deviation_created_at
        , recipes.main_recipe_id
        , deviations.deviation_version
    from deviations
    left join menus
        on
            deviations.menu_week = menus.menu_week
            and deviations.menu_year = menus.menu_year
            and deviations.product_variation_id = menus.product_variation_id
            and deviations.company_id = menus.company_id
    left join products
        on
            deviations.product_variation_id = products.product_variation_id
            and deviations.company_id = products.company_id
    left join recipes
        on menus.recipe_id = recipes.recipe_id
    where products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
)

-- For each preselector output, find the most recent deviations that were created at or before the time of the output for that billing agreement
-- The reason for doing this is because for each preselector output, we want to compare this output with the deviations that were available at the time of the output.
-- This way we can then calculate how many times a recipe output by the preselector was currently visible to the customer in either future or past menu weeks.
, join_deviations_and_preselector as (
    select
        preselector.pk_fact_preselector
        , preselector.main_recipe_id
        , preselector.menu_week_monday_date as preselector_menu_week_monday_date
        , deviations.menu_week_monday_date as deviation_menu_week_monday_date
        , deviations.main_recipe_id        as deviation_main_recipe_id
        , deviations.deviation_version
        -- Rank the deviation versions per menu week at the time of the output
        -- The reason for doing this is because we want to later filter on the most recent deviation version that was available at the time of the preselector output
        , dense_rank() over (
            partition by preselector.pk_fact_preselector, deviations.menu_week_monday_date
            order by deviations.deviation_version desc
        ) as deviation_rank
    from preselector_output_generate_keys as preselector
    left join join_deviations_and_recipes as deviations
        on
            preselector.billing_agreement_id = deviations.billing_agreement_id
            -- Add 1 minute to the preselector created_at to account for the fact that the deviation is created slightly after the preselector output
            and dateadd(minute, 1, preselector.created_at) >= deviations.deviation_created_at
    where
        -- Only consider deviations from the previous 6 menu weeks and future menu weeks
        deviations.menu_week_monday_date >= date_sub(preselector.menu_week_monday_date, 42)
        -- Exclude deviations from the same week as the preselector output, as the output will overwrite the deviation for that week
        and deviations.menu_week_monday_date != preselector.menu_week_monday_date
    -- Only consider the latest deviation version at the time of the output
    qualify deviation_rank = 1
)

-- Calculate the metrics for repeat selection
, repeat_selection_metrics as (
    select
        pk_fact_preselector
        -- Calculate the number of menu weeks in the window we are comparing against,
        -- this is because it can change based on the week the preselector was outputting for
        , count(
            distinct deviation_menu_week_monday_date
        ) as menu_week_window
        -- Calculate the number of distinct deviation_menu_week_monday_date where the main recipe that the preselector output
        -- is the same as the main recipe that was in the latest deviation at the time, meaning a repeat selection.
        -- We are calculating the number of distinct deviation_menu_week_monday_date because we want to know how many times the same recipe was already present in
        -- other menu weeks at that time. If we counted the preselector_menu_week_monday_date we would only count a recipe once
        -- for each preselector output, even if it was present in other menu weeks at that time as well.
        , count(
            distinct
            case
                when main_recipe_id = deviation_main_recipe_id
                then deviation_menu_week_monday_date
            end
        ) as repeat_weeks
    from join_deviations_and_preselector
    group by pk_fact_preselector
)

-- Join the repeat selection metrics back with the preselector output
, join_repeat_selection_metrics_with_preselector_output as (

    select
        preselector_output_generate_keys.*
        , coalesce(repeat_selection_metrics.repeat_weeks, 0) as repeat_weeks
        , coalesce(repeat_selection_metrics.menu_week_window, 0) as menu_week_window
        , coalesce(try_divide(repeat_selection_metrics.repeat_weeks, repeat_selection_metrics.menu_week_window), 0) as repeat_weeks_percentage
    from preselector_output_generate_keys
    left join repeat_selection_metrics
        on preselector_output_generate_keys.pk_fact_preselector = repeat_selection_metrics.pk_fact_preselector
)

select * from join_repeat_selection_metrics_with_preselector_output
