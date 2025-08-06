with

preselector_successful_output_dishes as (

    select * from {{ ref('int_mloutputs_preselector_successful_realtime_output_explode_product_variations') }}

)

, preselector_successful_output_mealbox as (

    select * from {{ ref('mloutputs__preselector_successful_realtime_output') }}

)

, deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

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

, portions as (

    select * from {{ ref('dim_portions') }}

)

, ingredient_combinations as (

    select * from {{ ref('int_recipes_with_ingredient_combinations') }}

)


-- Get the recipe and main recipe id for each dish in the preselector dishes output
, join_recipe_and_main_recipe_id_to_preselector_dishes as (

    select
        -- Timestamps
        preselector_successful_output_dishes.created_at

        -- Identifiers
        , preselector_successful_output_dishes.preselector_output_id
        , preselector_successful_output_dishes.billing_agreement_id
        , preselector_successful_output_dishes.company_id
        , preselector_successful_output_dishes.product_variation_id
        , menus.recipe_id
        , recipes.main_recipe_id
        , recipes.recipe_main_ingredient_name_english
        , preselector_successful_output_dishes.model_version_commit_sha

        -- Versioning
        , preselector_successful_output_dishes.menu_week_output_version
        , preselector_successful_output_dishes.is_most_recent_output

        -- Request details
        , preselector_successful_output_dishes.menu_year
        , preselector_successful_output_dishes.menu_week
        , preselector_successful_output_dishes.menu_week_monday_date
        , {{ get_financial_date_from_monday_date('preselector_successful_output_dishes.menu_week_monday_date') }} as menu_week_financial_date
        , preselector_successful_output_dishes.portions
        , preselector_successful_output_dishes.meals
        , preselector_successful_output_dishes.has_data_processing_consent

        -- Preferences
        , preselector_successful_output_dishes.concept_preference_ids
        , preselector_successful_output_dishes.taste_preference_ids
        , preselector_successful_output_dishes.taste_preference_compliancy_code

    from preselector_successful_output_dishes
    left join companies
        on preselector_successful_output_dishes.company_id = companies.company_id
    left join menus
        on
            preselector_successful_output_dishes.menu_year = menus.menu_year
            and preselector_successful_output_dishes.menu_week = menus.menu_week
            and preselector_successful_output_dishes.product_variation_id = menus.product_variation_id
            and preselector_successful_output_dishes.company_id = menus.company_id
    left join recipes
        on
            menus.recipe_id = recipes.recipe_id
            and companies.language_id = recipes.language_id
    -- Only select recipes that are selected for the menu, as we are joining in future menu weeks where the recipes are not finalised yet
    where menus.is_selected_menu = true
    and menus.recipe_id is not null
)

-- TODO: add this as an intermediate table
-- To create the repeat dish metrics, we need to first join the deviations with the recipes
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
, join_deviations_and_preselector_dishes as (
    select
        preselector.preselector_output_id
        , preselector.main_recipe_id
        , preselector.menu_week_monday_date as preselector_menu_week_monday_date
        , deviations.menu_week_monday_date as deviation_menu_week_monday_date
        , deviations.main_recipe_id        as deviation_main_recipe_id
        , deviations.deviation_version
        -- Rank the deviation versions per menu week at the time of the output
        -- The reason for doing this is because we want to later filter on the most recent deviation version that was available at the time of the preselector output
        , dense_rank() over (
            partition by
                preselector.preselector_output_id
                , preselector.main_recipe_id
                , deviations.menu_week_monday_date
            order by deviations.deviation_version desc
        ) as deviation_rank
    from join_recipe_and_main_recipe_id_to_preselector_dishes as preselector
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
        preselector_output_id
        , main_recipe_id
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
    from join_deviations_and_preselector_dishes
    group by preselector_output_id, main_recipe_id
)

-- Join the repeat selection metrics back with the preselector output
, join_repeat_selection_metrics_with_preselector_dishes as (

    select
        join_recipe_and_main_recipe_id_to_preselector_dishes.*
        , coalesce(repeat_selection_metrics.repeat_weeks, 0) as repeat_weeks
        , coalesce(repeat_selection_metrics.menu_week_window, 0) as menu_week_window
        , coalesce(try_divide(repeat_selection_metrics.repeat_weeks, repeat_selection_metrics.menu_week_window), 0) as repeat_weeks_percentage
    from join_recipe_and_main_recipe_id_to_preselector_dishes
    left join repeat_selection_metrics
        on
            join_recipe_and_main_recipe_id_to_preselector_dishes.preselector_output_id = repeat_selection_metrics.preselector_output_id
            and join_recipe_and_main_recipe_id_to_preselector_dishes.main_recipe_id = repeat_selection_metrics.main_recipe_id
)

-- We want to have both the dishes and also the mealbox data in this table, such that we can calculate the mealbox level metrics as well as the dish level metrics
-- therefore we union the dishes which are created above, with the the mealbox data which is created below
, union_preselector_dishes_with_mealbox as (

    select
    *
    -- List of main recipe ids, not relevant for dishes.
    , null as main_recipe_ids
    -- Distinguishing between dishes and mealboxes
    , true as is_dish
    from join_repeat_selection_metrics_with_preselector_dishes

    union all

    select
        -- Timestamps
        preselector_successful_output_mealbox.created_at
        -- Identifiers
        , preselector_successful_output_mealbox.preselector_output_id
        , preselector_successful_output_mealbox.billing_agreement_id
        , preselector_successful_output_mealbox.company_id
        -- Dish level IDs not relevant for mealboxes
        , null as product_variation_id
        , null as recipe_id
        , null as main_recipe_id
        , null as recipe_main_ingredient_name_english
        -- Identifiers
        , preselector_successful_output_mealbox.model_version_commit_sha
        -- Versioning
        , preselector_successful_output_mealbox.menu_week_output_version
        , preselector_successful_output_mealbox.is_most_recent_output
        -- Request details
        , preselector_successful_output_mealbox.menu_year
        , preselector_successful_output_mealbox.menu_week
        , preselector_successful_output_mealbox.menu_week_monday_date
        , {{ get_financial_date_from_monday_date('preselector_successful_output_mealbox.menu_week_monday_date') }} as menu_week_financial_date
        , preselector_successful_output_mealbox.portions
        , preselector_successful_output_mealbox.meals
        , preselector_successful_output_mealbox.has_data_processing_consent
        -- Preferences
        , preselector_successful_output_mealbox.concept_preference_ids
        , preselector_successful_output_mealbox.taste_preference_ids
        , preselector_successful_output_mealbox.taste_preference_compliancy_code
        -- Dish repeat selection metrics which are not relevant for mealboxes
        , null as repeat_weeks
        , null as menu_week_window
        , null as repeat_weeks_percentage
        -- Identifiers
        , preselector_successful_output_mealbox.main_recipe_ids
        -- Distinguishing between dishes and mealboxes
        , false as is_dish
    from preselector_successful_output_mealbox
)

-- Calculate the mealbox level metrics that we want to report on
, add_mealbox_level_metrics as (
    select
        union_preselector_dishes_with_mealbox.*
        -- Dish rotation (aka. repeat dishes)
        , case
            when is_dish = false then 1-avg(repeat_weeks_percentage) over (
                partition by billing_agreement_id, preselector_output_id
            )
            when is_dish = true then 1-repeat_weeks_percentage
            else null end as rotation_score
        , case
            when rotation_score = 1 then "Very High"
            when rotation_score >= 0.9 then "High"
            when rotation_score >= 0.8 then "Medium"
            when rotation_score >= 0.6 then "Low"
            when rotation_score >= 0 then "Very Low"
            else null
        end as rotation_score_group
        , case
            when rotation_score_group = "Very High" then 1
            when rotation_score_group = "High" then 2
            when rotation_score_group = "Medium" then 3
            when rotation_score_group = "Low" then 4
            when rotation_score_group = "Very Low" then 5
            else 6
        end as rotation_score_group_number
        -- Protein variation
        , case
            when is_dish = false then size(collect_set(recipe_main_ingredient_name_english) over (
                partition by billing_agreement_id, preselector_output_id
            ))
            else null
          end as number_of_unique_main_ingredients
        , case
            when is_dish = false then number_of_unique_main_ingredients/meals
            else null
        end as main_ingredient_variation_score
        , case
            when main_ingredient_variation_score = 1 then "Very High"
            when main_ingredient_variation_score >= 0.75 then "High"
            when main_ingredient_variation_score >= 0.6 then "Medium"
            when main_ingredient_variation_score >= 0.5 then "Low"
            when main_ingredient_variation_score >= 0 then "Very Low"
            else null
        end as main_ingredient_variation_score_group
        , case
            when main_ingredient_variation_score_group = "Very High" then 1
            when main_ingredient_variation_score_group = "High" then 2
            when main_ingredient_variation_score_group = "Medium" then 3
            when main_ingredient_variation_score_group = "Low" then 4
            when main_ingredient_variation_score_group = "Very Low" then 5
            else 6
        end as main_ingredient_variation_score_group_number
        -- Overall selection quality metric
        , main_ingredient_variation_score * rotation_score as combined_rotation_variation_score
        , case
            when combined_rotation_variation_score = 1 then "Very High"
            when combined_rotation_variation_score >= 0.8 then "High"
            when combined_rotation_variation_score >= 0.6 then "Medium"
            when combined_rotation_variation_score >= 0.4 then "Low"
            when combined_rotation_variation_score >= 0 then "Very Low"
            else null
        end as combined_rotation_variation_score_group
        , case
            when combined_rotation_variation_score_group = "Very High" then 1
            when combined_rotation_variation_score_group = "High" then 2
            when combined_rotation_variation_score_group = "Medium" then 3
            when combined_rotation_variation_score_group = "Low" then 4
            when combined_rotation_variation_score_group = "Very Low" then 5
            else 6
        end as combined_rotation_variation_score_group_number
    from union_preselector_dishes_with_mealbox
)

, add_keys as (

    select
        add_mealbox_level_metrics.*
        , ingredient_combinations.ingredient_combination_id
        , companies.language_id
        -- Primary key
        , md5(
            cast(concat(
                add_mealbox_level_metrics.billing_agreement_id
                , coalesce(add_mealbox_level_metrics.product_variation_id, 'mealbox')
                , add_mealbox_level_metrics.created_at
                , add_mealbox_level_metrics.menu_week_monday_date
            ) as string)
        ) as pk_fact_preselector
        -- Foreign keys
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
        , md5(add_mealbox_level_metrics.company_id) as fk_dim_companies
        , md5(
            concat(add_mealbox_level_metrics.recipe_id, companies.language_id)
        ) as fk_dim_recipes
        , cast(
            date_format(add_mealbox_level_metrics.created_at, 'yyyyMMdd') as int
        ) as fk_dim_dates_created_at
        , cast(
            date_format(add_mealbox_level_metrics.menu_week_financial_date, 'yyyyMMdd') as int
        ) as fk_dim_dates
        , date_format(add_mealbox_level_metrics.created_at, 'HHmm') as fk_dim_time_created_at
        , md5(
            add_mealbox_level_metrics.model_version_commit_sha
        )                                      as fk_dim_preselector_versions
        , case when is_dish = true then
            products_dish.pk_dim_products
        else
            products_mealbox.pk_dim_products
        end as fk_dim_products
        , case when is_dish = true then
            portions_dish.pk_dim_portions
        else
            portions_mealbox.pk_dim_portions
        end as fk_dim_portions
        , coalesce(md5(concat(ingredient_combinations.ingredient_combination_id, companies.language_id)), '0') as fk_dim_ingredient_combinations
    from add_mealbox_level_metrics
    left join companies
        on add_mealbox_level_metrics.company_id = companies.company_id
    left join agreements
        on
            add_mealbox_level_metrics.billing_agreement_id = agreements.billing_agreement_id
            and add_mealbox_level_metrics.created_at >= agreements.valid_from
            and add_mealbox_level_metrics.created_at < agreements.valid_to
    -- The connection to dim_products is different for dishes and mealboxes, as the preselector doesn't output the
    -- product_variation_id for the mealbox, so I have to join it on the company, product_name, type, meals, and portions
    left join products as products_mealbox
        on
            add_mealbox_level_metrics.company_id = products_mealbox.company_id
            and products_mealbox.product_type_id = '{{ var("mealbox_product_type_id") }}'
            and products_mealbox.product_id = '{{ var("onesub_product_id") }}'
            and add_mealbox_level_metrics.meals = products_mealbox.meals
            and add_mealbox_level_metrics.portions = products_mealbox.portions
            and add_mealbox_level_metrics.is_dish = false
    left join products as products_dish
        on
            add_mealbox_level_metrics.company_id = products_dish.company_id
            and add_mealbox_level_metrics.product_variation_id = products_dish.product_variation_id
            and add_mealbox_level_metrics.is_dish = true
    left join portions as portions_mealbox
        on products_mealbox.portion_name = portions_mealbox.portion_name_local
        and companies.language_id = portions_mealbox.language_id
    left join portions as portions_dish
        on products_dish.portion_name = portions_dish.portion_name_local
        and companies.language_id = portions_dish.language_id
    left join billing_agreement_preferences
        on agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id
    left join ingredient_combinations
        on add_mealbox_level_metrics.recipe_id = ingredient_combinations.recipe_id
        and portions_dish.portion_id = ingredient_combinations.portion_id
        and companies.language_id = ingredient_combinations.language_id
)

select * from add_keys
