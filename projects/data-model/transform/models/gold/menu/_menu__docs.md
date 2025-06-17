# Fact Recipe Reactions
{% docs column__pk_fact_recipe_reactions %}

Primary key of the fact_recipe_reactions table.

{% enddocs %}

# Dim Recipe Reaction Types
{% docs column__pk_dim_recipe_reaction_types %}

Primary key of the fact_recipe_reaction_types table.

{% enddocs %}

# Fact Menus
{% docs column__pk_fact_menus %}

Primary key of the fact_menus table. 

Created by concatenating weekly_menu_id, menu_id, product_variation_id, recipe_id, menu_recipe_id, potion_id, menu_number_days and menu_recipe_order.

{% enddocs %}

{% docs column__is_dish %}

Is true if the product variation is a dish (i.e., velg & vrak product type). Some groceries and other product types also have recipes.

{% enddocs %}

{% docs column__portion_id_menus %}

portion_id obtained from the menu variations.

{% enddocs %}

{% docs column__portion_id_products %}

portion_id obtained from product variations.

{% enddocs %}

{% docs column__recipe_planned_cost %}

The menu planning recipe cost, calculated as the sum of the cost of the recipe ingredients based on the exact quantity used in the recipe.

{% enddocs %}

{% docs column__recipe_planned_cost_whole_units %}

The menu planning recipe cost based on ingredient quantity in whole units. It is calculated as the sum of the cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer.

{% enddocs %}

{% docs column__recipe_expected_cost %}

The expected recipe cost, calculated as the sum of the cost of the recipe ingredients based on the exact quantity used in the recipe. This is the expected purchasing cost which takes any special cost (variable cost unit) into account if set.

{% enddocs %}

{% docs column__recipe_expected_cost_whole_units %}

The expected recipe cost based on ingredient quantity in whole units. It is calculated as the sum of the cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer. This is the expected purchasing cost which takes any special cost (variable cost unit) into account if set.

{% enddocs %}

{% docs column__recipe_actual_cost %}

The actual recipe cost, calculated as the sum of the cost of the ingredients in the recipe based on the exact quantity used in the recipe. The cost used for the calculation is the ingredient cost on the purchase order.

{% enddocs %}

{% docs column__recipe_actual_cost_whole_units %}

The actual recipe cost based on ingredient quantity in whole units. It is calculated as the sum of the cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer. The cost used for the calculation is the ingredient cost on the purchase order.

{% enddocs %}

# Dim Recipes
{% docs column__pk_dim_recipes %}

...

{% enddocs %}

{% docs column__recipe_difficulty_name %}

...

{% enddocs %}

{% docs column__recipe_main_ingredient_name_local %}

The main ingredient of the recipe in the local language.

{% enddocs %}

{% docs column__recipe_main_ingredient_name_english %}

The main ingredient of the recipe in english language.

{% enddocs %}

{% docs column__main_recipe_name %}

...

{% enddocs %}

{% docs column__is_main_recipe %}

...

{% enddocs %}

{% docs column__cooking_time_sorting %}

A column used for sorting the column cooking_time. It is calculated as cooking_time_from*1000+cooking_time_to.

{% enddocs %}

# Dim Products
{% docs column__pk_dim_products %}

Primary key of dim_products. It is a composite of product_variation_id and company_id.

{% enddocs %}

{% docs column__is_financial %}

product_type_name is renamed from Financial to Mealbox in Gold to avoid confusion when filtering, so this is a flag to indicate whether this product originally had a Financial product_type_name in the silver layer.

{% enddocs %}

# Dim Taxonomies
{% docs column__pk_dim_taxonomies %}

Primary key of the dim_taxonomies table.

{% enddocs %}

{% docs column__taxonomy_name_local %}

The name of the taxonomy in the local language.

{% enddocs %}

{% docs column__taxonomy_name_english %}

The name of the taxonomy in English.

{% enddocs %}

{% docs column__taxonomy_status_name_local %}

The name of the status for each taxonomy in the local language.

{% enddocs %}

{% docs column__taxonomy_status_name_english %}

The name of the status for each taxonomy in English.

{% enddocs %}


# Dim Ingredients
{% docs column__pk_dim_ingredients %}

Primary key of the dim_ingredients table.

{% enddocs %}

{% docs column__ingredient_full_name %}

The ingredient name in local language with size and unit label appended.

{% enddocs %}

{% docs column__main_group %}

Ingredient category name of the main group of an ingredient.

{% enddocs %}

{% docs column__category_group%}

Ingredient category name of the category group of an ingredient.

{% enddocs %}

{% docs column__product_group %}

Ingredient category name of the product group of an ingredient.

{% enddocs %}

{% docs column__category_level1 %}

Ingredient category id of the first level of the ingredient category hierarchy.

{% enddocs %}

{% docs column__category_level2 %}

Ingredient category id of the second level of the ingredient category hierarchy.

{% enddocs %}

{% docs column__category_level3 %}

Ingredient category id of the third level of the ingredient category hierarchy.

{% enddocs %}

{% docs column__category_level4 %}

Ingredient category id of the fourth level of the ingredient category hierarchy.

{% enddocs %}

{% docs column__category_level5 %}

Ingredient category id of the fifth level of the ingredient category hierarchy.

{% enddocs %}


# Bridge Dim Recipes Dim Taxonomies

{% docs column__pk_bridge_dim_recipes_dim_taxonomies %}

Primary key of the bridge table which connects dim_recipes and dim_taxonomies

{% enddocs %}

# Bridge Recipes Ingredients
{% docs column__pk_bridge_recipes_ingredients %}

Primary key of the bridge table which connects dim_recipes and dim_ingredients

{% enddocs %}

# Dim Portions

{% docs column__pk_dim_portions %}

Primary key of dim_portions. It is a composite o portion_id and language_id.

{% enddocs %}

{% docs column__portion_status_name_local %}

The name of the status for each portion on the local language.

{% enddocs %}

{% docs column__portion_status_name_english %}

The name of the status for each portion on English.

{% enddocs %}

{% docs column__portions %}

Number of portions in the products. Plus size portions will have the same value as normal size portions.
portion_name can be used to get information about plus size portions.

{% enddocs %}

{% docs column__portion_name_local %}

The name of the number of portions on the local language.

{% enddocs %}

{% docs column__portion_name_english %}

The name of the number of portions on English.

{% enddocs %}

# Fact Recipes Ingredients
{% docs column__pk_fact_recipes_ingredients %}

Primary key of Fact Recipes Ingredients. Is a composite of menu_variation_id, menu_recipe_id, recipe_portion_id and weekly_ingredient_id.

{% enddocs %}

{% docs column__portion_name_products %}

The name of the number of portions in local language.

{% enddocs %}

{% docs column__is_recipe_main_carbohydrate %}

Indicates if the ingredient is the main carbohydrate of the actual recipe (True) or not (False).

{% enddocs %}

{% docs column__is_recipe_main_protein %}

Indicates if the ingredient is the main protein of the actual recipe (True) or not (False).

{% enddocs %}

{% docs column__ingredient_nutrition_units %}

The amount of the actual ingredient which is included in nutritional calculations. The unit is specified by the unit label.

{% enddocs %}

{% docs column__ingredient_order_quantity %}

The amount of the actual ingredient to be ordered for this recipe. The unit is specified by the unit label.

{% enddocs %}

{% docs column__ingredient_planned_cost %}

The menu planning purchasing cost for one unit of the ingredient, valid for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_planned_cost %}

The menu planning purchasing cost for the total ingredient amount of the recipe, valid for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_planned_cost_whole_units %}

The menu planning purchasing cost for the total ingredient amount of the recipe, rounded up to the nearest integer, valid for the menu week of the recipe.

{% enddocs %}

{% docs column__ingredient_expected_cost %}

The expected purchasing cost for one unit of the ingredient, valid for the menu week of the recipe. The expected cost might have a discount that is not to be included in the menu planning process.

{% enddocs %}

{% docs column__total_ingredient_expected_cost %}

The expected purchasing cost for the total ingredient amount of the recipe, valid for the menu week of the recipe. The expected cost might have a discount that is not to be included in the menu planning process.

{% enddocs %}

{% docs column__total_ingredient_expected_cost_whole_units %}

The expected purchasing cost for the total ingredient amount of the recipe, rounded up to the nearest integer, which is valid for the menu week of the recipe. The expected cost might have a discount that is not to be included in the menu planning process.

{% enddocs %}

{% docs column__ingredient_actual_cost %}

The actual purchasing cost for one unit of the ingredient, fetched from the purchasing order(s) for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_actual_cost %}

The actual purchasing cost for the total ingredient amount of the recipe, fetched from the purchasing order(s) for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_actual_cost_whole_units %}

The actual purchasing cost for the total ingredient amount of the recipe, fetched from the purchasing order(s) for the menu week of the recipe. The total ingredient amount is rounded up to the nearest integer.

{% enddocs %}

# Meals

{% docs column__pk_dim_meals %}

Primary key of dim_meals. Has the same value as the meals column.

{% enddocs %}

{% docs column__meals %}

Number of meals related to the product. Mainly relevant for mealbox product, but does also hold information about meals for other product types such as Velg&Vrak and Groceries. However, for these meals are usually 1.

{% enddocs %}