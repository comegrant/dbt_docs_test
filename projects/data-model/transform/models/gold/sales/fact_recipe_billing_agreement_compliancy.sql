with

dim_billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

)

, dim_recipes as (

    select * from {{ ref('dim_recipes') }}

)

, recipe_preferences as (

    select * from {{ ref('int_recipe_preferences_unioned') }}

)

, preference_combinations as (

    select * from {{ ref('dim_all_preference_combinations') }}

)

, menu_weeks as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, preselector_output as (

    select * from {{ ref('mloutputs__preselector_successful_realtime_output') }}

)

, billing_agreement_week as (
    select
        dim_billing_agreements.pk_dim_billing_agreements                        as fk_dim_billing_agreements
        , dim_billing_agreements.company_id
        , dim_billing_agreements.billing_agreement_id
        , agreement_preferences.preference_combination_id                       as billing_agreement_preference_combination_id
        , preference_combinations.allergen_preference_id_list                   as agreement_allergen_preference_id_list
        , preference_combinations.concept_preference_id_list                    as agreement_concept_preference_id_list
        , preference_combinations.taste_preferences_excluding_allergens_id_list as agreement_taste_preferences_excluding_allergens_id_list
        , preselector_output.menu_week
        , preselector_output.menu_year
    from preselector_output
    left join dim_billing_agreements
        on
            preselector_output.billing_agreement_id = dim_billing_agreements.billing_agreement_id
            and preselector_output.created_at >= dim_billing_agreements.valid_from
            and preselector_output.created_at <= dim_billing_agreements.valid_to
    left join agreement_preferences
        on
            dim_billing_agreements.billing_agreement_preferences_updated_id
            = agreement_preferences.billing_agreement_preferences_updated_id
    left join preference_combinations
        on
            agreement_preferences.preference_combination_id
            = preference_combinations.pk_preference_combination_id
    where
        dim_billing_agreements.billing_agreement_preferences_updated_id is not null
        and preselector_output.is_latest_menu_week_output_version is true
)

, recipe_week as (
    select
        dim_recipes.pk_dim_recipes                                              as fk_dim_recipes
        , dim_recipes.recipe_id
        , dim_recipes.language_id
        , recipe_preferences.preference_combination_id                          as recipe_preference_combination_id
        , preference_combinations.allergen_preference_id_list                   as recipe_allergen_preference_id_list
        , preference_combinations.concept_preference_id_list                    as recipe_concept_preference_id_list
        , preference_combinations.taste_preferences_excluding_allergens_id_list as recipe_taste_preferences_excluding_allergens_id_list
        , menu_weeks.menu_week                                                  as recipe_menu_week
        , menu_weeks.menu_year                                                  as recipe_menu_year
        , menu_weeks.company_id                                                 as recipe_company_id
    from menu_weeks
    left join dim_recipes
        on
            menu_weeks.recipe_id = dim_recipes.recipe_id
            and menu_weeks.language_id = dim_recipes.language_id
    left join recipe_preferences
        on dim_recipes.recipe_id = recipe_preferences.recipe_id
    left join preference_combinations
        on recipe_preferences.preference_combination_id = preference_combinations.pk_preference_combination_id
    where
        dim_recipes.recipe_id is not null
)

, cross_join_agreements_and_recipes as (
    select distinct
        billing_agreement_week.*
        , recipe_week.*
    from billing_agreement_week
    inner join recipe_week
        on
            billing_agreement_week.menu_week = recipe_week.recipe_menu_week
            and billing_agreement_week.menu_year = recipe_week.recipe_menu_year
            and billing_agreement_week.company_id = recipe_week.recipe_company_id
)

, calculate_compliancy_and_date as (
    select
        *
        , {{ get_iso_week_start_date('menu_year', 'menu_week') }} as menu_week_monday_date
        , case
            when
                arrays_overlap(agreement_allergen_preference_id_list, recipe_allergen_preference_id_list)
                then 1
            when
                arrays_overlap(
                    agreement_taste_preferences_excluding_allergens_id_list
                    , recipe_taste_preferences_excluding_allergens_id_list
                )
                or (
                    array_size(
                        array_intersect(
                            agreement_concept_preference_id_list, recipe_concept_preference_id_list
                        )
                    )
                )
                = 0
                then 2
            else 3
        end as compliancy_level
    from cross_join_agreements_and_recipes
)

, generate_keys as (
    select
        md5(
            concat_ws(
                '-'
                , billing_agreement_id
                , recipe_id
                , language_id
                , menu_week
                , menu_year
                , company_id
            )
        )                 as pk_fact_recipe_billing_agreement_compliancy
        , md5(company_id) as fk_dim_companies
        , cast(
            date_format(menu_week_monday_date, 'yyyyMMdd') as int
        )                 as fk_dim_dates
        , fk_dim_billing_agreements
        , fk_dim_recipes
        , billing_agreement_id
        , billing_agreement_preference_combination_id
        , recipe_id
        , recipe_preference_combination_id
        , compliancy_level
    from calculate_compliancy_and_date
)

select * from generate_keys
