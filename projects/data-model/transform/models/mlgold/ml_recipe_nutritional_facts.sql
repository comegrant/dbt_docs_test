with

recipes as (
    select * from {{ ref('dim_recipes') }}
)

, recipe_portions as (
    select * from {{ ref('pim__recipe_portions') }}
)

, portions as (
    select * from {{ ref('pim__portions') }}
)

, portion_translations as (
    select * from {{ ref('pim__portion_translations') }}
)

, ingredient_sections as (
    select * from {{ ref('pim__chef_ingredient_sections') }}
)

, chef_ingredients as (
    select * from {{ ref('pim__chef_ingredients') }}
)

, generic_ingredients as (
    select * from {{ ref('pim__generic_ingredients') }}
)

, order_ingredients as (
    select * from {{ ref('pim__order_ingredients') }}
)

, ingredients as (
    select * from {{ ref('pim__ingredients') }}
)

, nutrient_info as (
    select
        ingredient_id
        , ingredient_nutrient_fact_id
        , ingredient_nutritional_value
    from {{ ref('pim__ingredient_nutrient_facts') }}
)

, nutrient_translations as (
    select * from {{ ref('pim__nutrient_fact_translations') }}
)

, nutrients as (
    select
        nutrient_info.ingredient_id
        , nutrient_info.ingredient_nutrient_fact_id           as nutrient_id
        , nutrient_info.ingredient_nutritional_value          as nutrient_value
        , nutrient_translations.ingredient_nutrient_fact_name as nutrient_name
        , nutrient_translations.language_id
    from nutrient_info
    left join nutrient_translations
        on nutrient_info.ingredient_nutrient_fact_id = nutrient_translations.ingredient_nutrient_fact_id
)

, nutrition_calculations as (
    select
        recipes.recipe_id
        , recipes.recipe_name
        , recipes.language_id
        , recipe_portions.recipe_portion_id
        , portions.portion_size
        , portion_translations.portion_name
        , sum(
            order_ingredients.nutrition_units
        ) as nutrition_units
        , coalesce(
            max(ingredients.ingredient_net_weight), -1
        ) as ingredient_net_weight
        , sum(
            coalesce(order_ingredients.nutrition_units, 0) * coalesce(ingredients.ingredient_net_weight, -1)
        ) as total_ingredient_net_weight
        -- grams per energy source
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_protein") }}' then nutrients.nutrient_value else 0 end
        ) as protein_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_carbs") }}' then nutrients.nutrient_value else 0 end
        ) as carbs_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_fat") }}' then nutrients.nutrient_value else 0 end
        ) as fat_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_saturated_fat") }}' then nutrients.nutrient_value else 0 end
        ) as sat_fat_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_sugar") }}' then nutrients.nutrient_value else 0 end
        ) as sugar_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_added_sugars") }}' then nutrients.nutrient_value else 0 end
        ) as sugar_added_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_fiber") }}' then nutrients.nutrient_value else 0 end
        ) as fiber_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_salt") }}' then nutrients.nutrient_value else 0 end
        ) as salt_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_added_salt") }}' then nutrients.nutrient_value else 0 end
        ) as salt_added_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_fresh_fruit_veg") }}' then nutrients.nutrient_value else 0 end
        ) as fg_fresh_g
        , sum(
            (coalesce(ingredients.ingredient_net_weight, -1) / 100.0)
            * coalesce(order_ingredients.nutrition_units, 0)
            * case when nutrients.nutrient_id = '{{ var("nutrient_id_processed_fruit_veg") }}' then nutrients.nutrient_value else 0 end
        ) as fg_proc_g
    from recipes
    left join recipe_portions
        on recipes.recipe_id = recipe_portions.recipe_id
    left join ingredient_sections
        on recipe_portions.recipe_portion_id = ingredient_sections.recipe_portion_id
    left join portions
        on recipe_portions.portion_id = portions.portion_id
    left join portion_translations
        on recipe_portions.portion_id = portion_translations.portion_id
    left join chef_ingredients
        on ingredient_sections.chef_ingredient_section_id = chef_ingredients.chef_ingredient_section_id
    left join generic_ingredients
        on chef_ingredients.generic_ingredient_id = generic_ingredients.generic_ingredient_id
    left join order_ingredients
        on chef_ingredients.order_ingredient_id = order_ingredients.order_ingredient_id
    left join ingredients
        on
            order_ingredients.ingredient_internal_reference = ingredients.ingredient_internal_reference
            and ingredients.is_active = true
    left join nutrients
        on
            ingredients.ingredient_id = nutrients.ingredient_id
            and recipes.language_id = nutrients.language_id
    where
        generic_ingredients.nutrition_calculation = 1
        and recipes.is_in_recipe_universe = true
    group by 1, 2, 3, 4, 5, 6
)

, recipe_portions_nutrition as (
    select
        recipe_id
        , recipe_name
        , language_id
        , recipe_portion_id
        , portion_size
        , portion_name
        , (protein_g / portion_size)     as protein_gram_per_portion
        , (carbs_g / portion_size)       as carbs_gram_per_portion
        , (fat_g / portion_size)         as fat_gram_per_portion
        , (sat_fat_g / portion_size)     as sat_fat_gram_per_portion
        , (sugar_g / portion_size)       as sugar_gram_per_portion
        , (sugar_added_g / portion_size) as sugar_added_gram_per_portion
        , (fiber_g / portion_size)       as fiber_gram_per_portion
        , (salt_g / portion_size)        as salt_gram_per_portion
        , (salt_added_g / portion_size)  as salt_added_gram_per_portion
        , (fg_fresh_g / portion_size)    as fg_fresh_gram_per_portion
        , (fg_proc_g / portion_size)     as fg_proc_gram_per_portion
        , (
            (4 * protein_gram_per_portion)
            + (4 * carbs_gram_per_portion)
            + (9 * fat_gram_per_portion)
        )                                as total_kcal_per_portion
    from nutrition_calculations
)

select * from recipe_portions_nutrition
