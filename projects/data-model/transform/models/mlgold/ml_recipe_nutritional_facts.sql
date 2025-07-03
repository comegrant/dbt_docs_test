with

recipes as (

    select * from {{ ref('dim_recipes') }}

)

, ingredients as (

    select * from {{ ref('dim_ingredients') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, recipe_ingredients as (

    select * from {{ ref('int_recipe_ingredients_joined') }}

)

, nutrients as (

    select * from {{ ref('pim__ingredient_nutrient_facts') }}

)

, recipe_ingredients_universe as (
    select
        recipes.recipe_id
        , recipes.language_id
        , portions.portion_name_local                   as portion_size
        , ingredients.ingredient_id
        , ingredients.ingredient_net_weight
        , recipe_ingredients.nutrition_units 
    from recipes
    left join recipe_ingredients
        on recipes.recipe_id = recipe_ingredients.recipe_id
    left join portions
        on recipe_ingredients.portion_id = portions.portion_id
    left join ingredients
        on recipe_ingredients.ingredient_id = ingredients.ingredient_id
    where
        recipes.is_in_recipe_universe = true
        and ingredients.is_active_ingredient = true
        and portions.language_id = recipes.language_id
        and ingredients.language_id = recipes.language_id
        and recipe_ingredients.language_id = recipes.language_id
        and portions.portion_name_local in (1, 2, 3, 4, 5, 6)

)

, recipe_ingredients_nutrition as (
    select
        recipe_ingredients_universe.recipe_id
        , recipe_ingredients_universe.portion_size
        , recipe_ingredients_universe.language_id
        , recipe_ingredients_universe.ingredient_id
        , recipe_ingredients_universe.nutrition_units
        , recipe_ingredients_universe.ingredient_net_weight as net_weight
        , nutrients.ingredient_nutritional_value            as nutrient_value
        , nutrients.ingredient_nutrient_fact_id             as nutrient_id
    from recipe_ingredients_universe
    left join nutrients
        on
            recipe_ingredients_universe.ingredient_id = nutrients.ingredient_id
    where nutrients.ingredient_nutrient_fact_id is not null
)

, nutrition_calculation as (
    select
        recipe_id
        , language_id
        , portion_size
        , sum(
            nutrition_units
        ) as nutrition_units
        , coalesce(
            max(net_weight), -1
        ) as ingredient_net_weight
        , sum(
            coalesce(nutrition_units, 0) * coalesce(net_weight, -1)
        ) as total_ingredient_net_weight
        -- grams per energy source
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_protein") }}' then nutrient_value else 0 end
        ) as protein_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_carbs") }}' then nutrient_value else 0 end
        ) as carbs_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_fat") }}' then nutrient_value else 0 end
        ) as fat_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_saturated_fat") }}' then nutrient_value else 0 end
        ) as sat_fat_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_sugar") }}' then nutrient_value else 0 end
        ) as sugar_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_added_sugars") }}' then nutrient_value else 0 end
        ) as sugar_added_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_fiber") }}' then nutrient_value else 0 end
        ) as fiber_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_salt") }}' then nutrient_value else 0 end
        ) as salt_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_added_salt") }}' then nutrient_value else 0 end
        ) as salt_added_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_fresh_fruit_veg") }}' then nutrient_value else 0 end
        ) as fg_fresh_g
        , sum(
            (coalesce(net_weight, -1) / 100.0)
            * coalesce(nutrition_units, 0)
            * case when nutrient_id = '{{ var("nutrient_id_processed_fruit_veg") }}' then nutrient_value else 0 end
        ) as fg_proc_g
    from recipe_ingredients_nutrition
    group by 1, 2, 3
)

, final as (
    select
        recipe_id
        , language_id
        , portion_size
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
    from nutrition_calculation
)

select * from final
