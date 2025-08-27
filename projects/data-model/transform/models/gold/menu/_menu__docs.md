# Dim Price Categories
{% docs column__pk_dim_price_categories %}

Primary key of the dim_price_categories table. Is a composite key of company_id, portion_id, price_category_level_id and valid_from.

{% enddocs %}

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

Is true if the product variation is a dish (i.e., velg & vrak product type) else false.

{% enddocs %}

{% docs column__portion_id_menus %}

portion_id obtained from the menu variations.

{% enddocs %}

{% docs column__portion_id_products %}

portion_id obtained from product variations.

{% enddocs %}

# Dim Recipes
{% docs column__pk_dim_recipes %}

The primary key of Dim Recipes. A composite key of recipe_id and langauge.

{% enddocs %}

{% docs column__recipe_main_ingredient_name_local %}

The main ingredient of the recipe in the local language.

{% enddocs %}

{% docs column__recipe_main_ingredient_name_english %}

The main ingredient of the recipe in english language.

{% enddocs %}

{% docs column__main_recipe_name %}

The name of the main recipe that the recipe is based on.

{% enddocs %}

{% docs column__is_main_recipe %}

True if recipe is main recipe. False if recipe is a variation of a main recipe.

{% enddocs %}

{% docs column__cooking_time_sorting %}

A column used for sorting the column cooking_time. It is calculated as cooking_time_from*1000+cooking_time_to.

{% enddocs %}

{% docs column__menu_week_count_main_recipe %}

Number of times the main recipe has been present in a menu week.

{% enddocs %}

{% docs column__previous_menu_week_main_recipe %}

The last menu week where the main recipe id was present in the format YYYYWW. For example week 3 in year 2025 will be 202503.

{% enddocs %}

{% docs column__weeks_since_first_menu_week_main_recipe %}

Number of weeks since the first time the main recipe id was present in a menu week. Is 0 if the main recipe was present for the first time in the menu for the current calendar week.

{% enddocs %}

# Dim Products
{% docs column__pk_dim_products %}

Primary key of dim_products. It is a composite of product_variation_id and company_id.

{% enddocs %}

{% docs column__is_financial %}

product_type_name is renamed from Financial to Mealbox in Gold to avoid confusion when filtering, so this is a flag to indicate whether this product originally had a Financial product_type_name in the silver layer.

{% enddocs %}

{% docs column__is_active_product %}

Used by the grocery team to perform troubleshooting related to which product that are visible on the web page. Seem to work like this: If sent_to_frontend = TRUE but is_active_product = FALSE, then the product will not be shown on the web page. However, no one seem to know 100% the logic behind this field and its not supposed to be in use anymore.

{% enddocs %}

{% docs column__picking_line_label %}

A label grouping product variations based on what part of the picking line the product belongs to.

{% enddocs %}

{% docs column__maximum_order_quantity %}

The maximum quantity a customer can order of the product variation in one week.

{% enddocs %}

{% docs column__vat_rate %}

The VAT category the product variation belongs to, represented as an whole number.

{% enddocs %}

{% docs column__has_extra_protein %}

Customers can choose to add extra protein to the products they order for an additional fee. This column is true if the product is of a variation that has extra protein.

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

{% docs column__ingredient_manufacturer_name %}

The name of the producing supplier, which can differ from the supplier.

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
{% docs column__pk_fact_recipe_ingredients %}

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

{% docs column__ingredient_weight_per_unit %}

The net weight (in grams) for one unit of the ingredient.

{% enddocs %}

{% docs column__total_ingredient_weight %}

The net weight (in grams) for the total ingredient amount in the recipe.

{% enddocs %}

{% docs column__total_ingredient_weight_whole_units %}

The net weight (in grams) of the ingredients, based on ingredient quantity in whole units.

{% enddocs %}

{% docs column__ingredient_planned_cost_per_unit %}

The menu planning purchasing cost for one unit of the ingredient, valid for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_planned_cost %}

The menu planning purchasing cost for the total ingredient amount in the recipe.

{% enddocs %}

{% docs column__total_ingredient_planned_cost_whole_units %}

The menu planning purchasing cost based on ingredient quantity in whole units. It is calculated as the sum of the cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer.

{% enddocs %}

{% docs column__ingredient_expected_cost_per_unit %}

The expected purchasing cost for one unit of the ingredient, valid for the menu week of the recipe. The expected cost includes discounts or other price adjustment (variable cost unit) if set, that is not included in the planned cost.

[//]: # (TODO: explain more about the purpose and use of this field vs the planned cost. Is this when there are discounts on on purchasing ingredients?)

{% enddocs %}

{% docs column__total_ingredient_expected_cost %}

The expected purchasing cost for the total ingredient amount in the recipe. The expected cost includes discounts or other price adjustment (variable cost unit) if set, as opposed to the planned cost that does not take the variable cost into account.

{% enddocs %}

{% docs column__total_ingredient_expected_cost_whole_units %}

The expected purchasing cost based on ingredient quantity in whole units. The expected cost includes discounts or other price adjustment (variable cost unit) if set, as opposed to the planned cost that does not take the variable cost into account. This field is calculated as the sum of the expected cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer.

{% enddocs %}

{% docs column__ingredient_actual_cost_per_unit %}

The actual purchasing cost for one unit of the ingredient, fetched from the purchasing order(s) for the menu week of the recipe.

{% enddocs %}

{% docs column__total_ingredient_actual_cost %}

The actual purchasing cost for the total ingredient amount of the recipe. It is fetched from the purchasing order(s) for the menu week of the recipe and calculated as the actual purchasing cost of the orders, divided by the quantity in the orders, multiplied with the ingredient quantity in the recipe. 

{% enddocs %}

{% docs column__total_ingredient_actual_cost_whole_units %}

The actual purchasing cost for the total ingredient amount of the recipe. It is fetched from the purchasing order(s) for the menu week of the recipe and calculated by dividing the actual purchasing cost of the orders by the ordered quantity, and then multipling it with the ingredient quantity (rounded up to whole units) in the recipe. 

{% enddocs %}

{% docs column__total_ingredient_co2_emissions %}

The total amount of registered co2 emissions (in kg) for the ingredients in the recipes. It is calculated by multipling the emission per kg of the ingredient with the net weight of on unit of the ingredient, and then multipling it with the quantity of the ingredient in the recipe.
Note: Not all ingredients have registered CO2 emission data. For meaningful analysis, use this field together with total_ingredient_weight_with_co2_data to calculate emissions per kg of food in the recipes.

{% enddocs %}

{% docs column__total_ingredient_co2_emissions_whole_units %}

The total amount of registered co2 emissions (in kg) for the ingredients in the recipes. It is calculated by multipling the emission per kg of the ingredient with the net weight of on unit of the ingredient, and then multipling it with the quantity (rounded up to whole units) of the ingredient in the recipe.
Note: Not all ingredients have registered CO2 emission data. For meaningful analysis, use this field together with total_ingredient_weight_with_co2_data_whole_units to calculate emissions per kg of food in the recipes.

{% enddocs %}

{% docs column__ingredient_weight_with_co2_data %}

The net weight (in grams) of one unit of the ingredient, that have registered CO2 emission data.

{% enddocs %}


{% docs column__total_ingredient_weight_with_co2_data %}

The total net weight (in grams) of ingredients in the recipe that have registered CO2 emission data.

{% enddocs %}

{% docs column__total_ingredient_weight_with_co2_data_whole_units %}

The total net weight (in grams) of ingredients in the recipe that have registered CO2 emission data, where the quantity of the ingredients is rounded up to whole units. 

{% enddocs %}

{% docs column__customer_ordered_product_variation_quantity %}

The number of units ordered by customers of the product variation in question. This column is not intended for aggregation, but can be useful for troubleshooting or validating customer order cost calculations.

{% enddocs %}

{% docs column__number_of_customer_orders %}
The number of customer orders that include the specified product variation. This column is not intended for aggregation, but can be useful for troubleshooting or validating customer order cost calculations.

{% enddocs %}

{% docs column__customer_ordered_ingredient_quantity %}

The total ingredient quantity ordered by customers, calculated as the number of ordered recipes, multiplied with the ingredient quantity within the recipe.

{% enddocs %}

{% docs column__customer_ordered_ingredient_weight %}

The net weight (in grams) of the ingredient ordered by customers, calculated as the number of ordered recipes multiplied by the net weight of the ingredient used in each recipe.

{% enddocs %}

{% docs column__customer_ordered_ingredient_weight_whole_units %}

The net weight (in grams) of the ingredient ordered by customers, where the quantity is rounded up to whole units as per recipe.

{% enddocs %}

{% docs column__customer_ordered_ingredient_planned_cost %}

The menu planning purchasing cost for the total ingredient amount ordered by customers.

{% enddocs %}

{% docs column__customer_ordered_ingredient_planned_cost_whole_units %}

The menu planning purchasing cost for the ingredient amount ordered by customers, based on ingredient quantity in whole units as per recipe. It is calculated as the sum of the cost of the recipe ingredients ordered based on the quantity used in the recipe rounded up to the nearest integer.

{% enddocs %}

{% docs column__customer_ordered_ingredient_expected_cost %}

The expected purchasing cost for the total ingredient amount ordered by customers. The expected cost includes discounts or other price adjustment (variable cost unit) if set, as opposed to the planned cost that does not take the variable cost into account.

{% enddocs %}

{% docs column__customer_ordered_ingredient_expected_cost_whole_units %}

The expected purchasing cost for the ingredient amount ordered by customers, based on ingredient quantity in whole units. The expected cost includes discounts or other price adjustment (variable cost unit) if set, as opposed to the planned cost that does not take the variable cost into account. This field is calculated as the sum of the expected cost of the recipe ingredients based on the quantity used in the recipe rounded up to the nearest integer.

{% enddocs %}

{% docs column__customer_ordered_ingredient_actual_cost %}

The actual purchasing cost for the total ingredient amount ordered by customers. It is fetched from the purchasing order(s) for the menu week of the recipe and calculated as the actual purchasing cost of the orders, divided by the quantity in the orders, multiplied with the ingredient quantity in the recipe and with the number of recipes ordered by customers. 

{% enddocs %}

{% docs column__customer_ordered_ingredient_actual_cost_whole_units %}

The actual purchasing cost for the total ingredient amount (in whole units per recipe) ordered by customers. It is fetched from the purchasing order(s) for the menu week of the recipe and calculated by dividing the actual purchasing cost of the orders by the ordered quantity, and then multipling it with the ingredient quantity (rounded up to whole units) in the recipe.

{% enddocs %}

{% docs column__customer_ordered_ingredient_co2_emissions %}

The total registered CO2 emissions (in kg) associated with the amount of the ingredient that was actually ordered, based on the ordered quantity and available emissions data.

The total amount of registered co2 emissions (in kg) for the ingredients ordered by customers. It is calculated by multipling the emission per kg of the ingredient with the net weight of on unit of the ingredient, and then multipling it with the quantity of the ingredient in the recipe as well as the number of recipes ordered by customers.
Note: Not all ingredients have registered CO2 emission data. For meaningful analysis, use this field together with customer_ordered_ingredient_weight_with_co2_data to calculate emissions per kg of food in the recipes.

{% enddocs %}

{% docs column__customer_ordered_ingredient_co2_emissions_whole_units %}

The total amount of registered co2 emissions (in kg) for the ingredients ordered by customers. It is calculated by multipling the emission per kg of the ingredient with the net weight of on unit of the ingredient, and then multipling it with the quantity (rounded up to whole units) of the ingredient in the recipe as well as the number of recipes ordered by customers.
Note: Not all ingredients have registered CO2 emission data. For meaningful analysis, use this field together with customer_ordered_ingredient_weight_with_co2_data_whole_units to calculate emissions per kg of food in the recipes.

{% enddocs %}

{% docs column__customer_ordered_ingredient_weight_with_co2_data %}

The total net weight (in grams) of ingredients that was orderd by customers and has registered CO2 emission data.

{% enddocs %}

{% docs column__customer_ordered_ingredient_weight_with_co2_data_whole_units %}

The total net weight (in grams) of ingredients that was orderd by customers and has registered CO2 emission data, where the quantity of the ingredients per recipe is rounded up to whole units. 

{% enddocs %}

# Meals

{% docs column__pk_dim_meals %}

Primary key of dim_meals. Has the same value as the meals column.

{% enddocs %}

{% docs column__meals %}

Number of meals related to the product. Mainly relevant for mealbox product, but does also hold information about meals for other product types such as Velg&Vrak and Groceries. However, for these meals are usually 1.

{% enddocs %}

# Dim Ingredient Combinations

{% docs column__pk_dim_ingredient_combinations %}

Primary key of dim_ingredient_combinations. Generated from ingredient_combination_id and language_id.

{% enddocs %}

{% docs column__ingredient_combination_id %}

Represents a specific set of ingredients grouped together, typically corresponding to a unique combination used in one or more recipes.

{% enddocs %}

{% docs column__ingredient_id_list_array %}

Contains a sorted array of all ingredient IDs that make up the combination, allowing for easy programmatic access and analysis of the individual ingredients involved.

{% enddocs %}

{% docs column__ingredient_id_combinations %}

Lists all ingredient IDs in the combination as a single, comma-separated string. Useful for display, reporting, or exporting purposes.

{% enddocs %}

{% docs column__ingredient_internal_reference_combinations %}

Lists the internal reference codes for each ingredient in the combination, separated by commas. 

{% enddocs %}

{% docs column__ingredient_name_combinations %}

Lists the names of all ingredients in the combination as a single, comma-separated string. This column is intended for user-facing applications, reporting, and easier interpretation of the ingredient combinations.

{% enddocs %}

# Bridge Ingredient Combinations Ingredients

{% docs column__pk_bridge_ingredient_combinations_ingredients %}

Primary key of bridge_ingredient_combinations_ingredients. Generated from ingredient_combination_id, ingredient_id and language_id.

{% enddocs %}
