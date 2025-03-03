# Common

# Weekly Menus
{% docs column__weekly_menu_id %}

The primary key of a menu week in the pim database.

{% enddocs %}


# Menus
{% docs column__menu_id %}

The primary key of a menu in the pim database. 

{% enddocs %}

{% docs column__is_selected_menu %}

If the menu is to be visible on frontend or not.

{% enddocs %}

{% docs column__is_locked_recipe %}

If the recipes of the menu are ready for purchasing.

{% enddocs %}

# Menu Variations
{% docs column__menu_variation_id %}

The primary key of a menu variation in the pim database. 

{% enddocs %}

{% docs column__menu_number_days %}

Number of days of the menu variation. Is 1 if Velg&Vrak variation.

{% enddocs %}

# Menu Recipes
{% docs column__menu_recipe_id %}

The primary key of menu recipes table in the pim database.

{% enddocs %}

{% docs column__menu_recipe_order %}

The order of the recipes in the menu. Is used to find which recipes that belong to mealboxes with X number of meals.

{% enddocs %}

# Recipes
{% docs column__recipe_id %}

The primary key of the recipes table in the pim database.

{% enddocs %}

{% docs column__main_recipe_id %}

The id of the main recipe that belongs to the recipe. Is null if recipe is a main recipe.

{% enddocs %}

{% docs column__main_recipe_created_by %}

The creator user name of the main recipe which the recipe variation was created from.

{% enddocs %}

{% docs column__main_recipe_created_at %}

The creation timestamp of the main recipe which the recipe variation was created from.

{% enddocs %}

{% docs column__source_created_at_recipes %}

The creator user name of the recipe variation.

{% enddocs %}

{% docs column__source_created_by_recipes %}

The creation timestamp of the recipe variation.

{% enddocs %}

{% docs column__source_updated_at_recipes %}

The user who made the last update to the recipe variation.

{% enddocs %}

{% docs column__source_updated_by_recipes %}

The last updated timestamp of the recipe variation.

{% enddocs %}

{% docs column__main_recipe_variation_suffix %}

Indicates the order number of the recipe variation created from one main recipe.

{% enddocs %}

{% docs column__main_recipe_variation_id %}

The main_recipe_id together with the main_recipe_variation_suffix.

{% enddocs %}

{% docs column__recipe_average_rating %}

The calculated average recipe rating from the customers for the recipe variation.

{% enddocs %}

{% docs column__recipe_shelf_life_days %}

The min shelf life of the ingredients in the recipe, indicating the max number of days the customer can wait before cooking the recipe.

{% enddocs %}

{% docs column__is_in_recipe_universe %}

Tells if the recipe is in the "recipe universe".

{% enddocs %}

{% docs column__recipe_status_code_id %}

...

{% enddocs %}

# Recipe Metadata
{% docs column__recipe_metadata_id %}

The primary key of recipe metadata table in the pim database.

{% enddocs %}

{% docs column__cooking_time_from %}

Minimum estimated time to make the recipe.

{% enddocs %}

{% docs column__cooking_time_to %}

Maximum estimated time to make the recipe.

{% enddocs %}

{% docs column__cooking_time %}

...

{% enddocs %}

{% docs column__recipe_photo %}

Photo of the recipe, used by frontend in web. Append string as suffix to the URL: https://pimimages.azureedge.net/images/resized/ to visualize images in i.e. a streamlit app.

{% enddocs %}

{% docs column__has_recipe_photo %}

...

{% enddocs %}

# Recipe Metadata Translations
{% docs column__recipe_name %}

The full name of the recipe. Used on frontend.

{% enddocs %}

{% docs column__recipe_photo_caption %}

Photo caption for the recipe photo. Used on frontend.

{% enddocs %}

{% docs column__roede_calculation_text %}

Special text for Roede recipes only. Used on frontend.

{% enddocs %}

{% docs column__recipe_extra_photo_caption %}

...

{% enddocs %}

{% docs column__recipe_general_text %}

General recipe comment. Used on frontend.

{% enddocs %}

{% docs column__recipe_description %}

Additional recipe description. Used on frontend.

{% enddocs %}

{% docs column__recipe_name_headline %}

The first part of the recipe name. Used on frontend.

{% enddocs %}

{% docs column__recipe_name_subheadline %}

The second part of the recipe name. Used on frontend.

{% enddocs %}

# Recipe Portions
{% docs column__recipe_portion_id %}

The primary key of the recipe portions table in the pim database.

{% enddocs %}

# Portions
{% docs column__portion_id %}

The primary key of the portions table in the pim database.

{% enddocs %}

{% docs column__portion_size %}

A numerical value for number of portions in a product or recipe.

{% enddocs %}

{% docs column__main_portion_id %}

...

{% enddocs %}

{% docs column__portion_status_code_id %}

The status code of the portions. 1 = Enabled, 2 = Disabled.

{% enddocs %}

# Recipe Ratings
{% docs column__recipe_rating %}

Rating (1-5) of the recipe given by the customer. If rating is 0 it means that the customer marked the recipe as not cooked.

{% enddocs %}

{% docs column__recipe_rating_score %}

The 1-5 rating converted to a 0-100 score.

{% enddocs %}

{% docs column__is_not_cooked_dish %}

The customer has selected that they have not cooked the dish (rating = 0).

{% enddocs %}

# Recipe Comments
{% docs column__recipe_comment %}

Free text comment made by a customer when rating the recipe.

{% enddocs %}

# Ingredient Categories
{% docs column__ingredient_category_id %} 

Identifier for each ingredient category 

{% enddocs %}

{% docs column__ingredient_category_name %} 

Name of ingredient category 

{% enddocs %}

{% docs column__parent_category_id %} 

Identifier for the ingredient category parent, indicating the hierarchy within ingredient categories. If null, the ingredient category is at the top of the hierarchy. 

{% enddocs %}

{% docs column__ingredient_category_description %} 

Description of the ingredient category, e.g. "main group"

{% enddocs %}
# Taxonomies
{% docs column__taxonomy_id %} 

Identifier for each taxonomy

{% enddocs %}

{% docs column__taxonomy_name %} 

Name of the taxonomy 

{% enddocs %}

{% docs column__taxonomy_type_id %}

Identifier for each taxonomy type

{% enddocs %}

{% docs column__taxonomy_type_name %}

The name of each taxonomy type 

{% enddocs %}

# Ingredients
{% docs column__ingredient_content %} 

Description of an ingredient's composition, e.g. "oil, egg, salt" for "mayonnaise" 

{% enddocs %}

# Generic Ingredient Translations
{% docs column__generic_ingredient_id %} 

Identifier for each generic ingredient, i.e. "soy-sauce"  

{% enddocs %}

{% docs column__generic_ingredient_name %}

Name of the generic ingredient 

{% enddocs %}

# Recipe Translations
{% docs column__recipe_translations_recipe_comment %} 

Internal comment on each recipe for menu team follow-up

{% enddocs %}

{% docs column__recipe_chef_tip %} 

Customer facing comment to the recipe

{% enddocs %}

# Portion Translations
{% docs column__portion_name %}

The text name of each portion size

{% enddocs %}

# Status Code Translations

{% docs column__pim_status_code_id %}

Identifier for the status

{% enddocs %}

{% docs column__status_code_name %}

The name of a status code.

{% enddocs %}

{% docs column__status_code_description %}

The description of the status code.

{% enddocs %}

# Recipe Favorites
{% docs column__recipe_favorite_id %}

The unique id of the recipe favortie rows in the source database.

The id will be regenerated for every inserted row meaning that if a customer like/dislike a recipe, then remove the like/dislike, and then like/dislike that recipe again the new row will get a new id.

{% enddocs %}

{% docs column__is_active_reaction %}

Is true if the reaction is still active for the recipe and false if the reaction has been removed.

{% enddocs %}

# Recipe Favorite Type

{% docs column__recipe_favorite_type_id %}

The unique id of the recipe favorite type in the source database.

{% enddocs %}

{% docs column__recipe_favorite_type_name %}

The name of the recipe type (can be favorite or dislike).
{% enddocs %}

# Not Organized
{% docs column__is_main_protein %} 

If an ingredient is used as the main protein in a recipe 

{% enddocs %}

{% docs column__is_main_carbohydrate %} 

If an ingredient is used as the main carbohydrate in a recipe 

{% enddocs %}

{% docs column__supplier_id %} 

Identifier for each ingredient supplier 

{% enddocs %}

{% docs column__ingredient_amount %} 

Amount of specific ingredient in a recipe variation 

{% enddocs %}

{% docs column__order_ingredient_id %} 

Identifier for an ingredient connected to a specific recipe variation 

{% enddocs %}

{% docs column__ingredient_id %} 

Identifier for an ingredient (connected to a supplier) 

{% enddocs %}

{% docs column__ingredient_internal_reference %} 

Used to join ingredient ids across different databases 

{% enddocs %}

{% docs column__chef_ingredient_section_id %}  

Identifier for each ingredient section, where an ingredient section is i.e. "pan-fried vegetables" contains the ingredient "carrots" and "broccoli"

{% enddocs %}

{% docs column__chef_ingredient_id %}

Identifier for chef ingredient, which is an instance of generic ingredient with a 1-1 mapping with order ingredient.

{% enddocs %}

{% docs column__ingredient_type %}

Parameter for production-line placement of products

{% enddocs %}

{% docs column__is_external_taxonomy %} 

If the taxonomy is used in frontend, i.e. a user-friendly taxonomy

{% enddocs %}

{% docs column__recipe_main_ingredient_id %}

Identifier for the recipe's main ingredient

{% enddocs %}

{% docs column__recipe_difficulty_level_id %}

Identifier for the recipes difficulty level

{% enddocs %}

{% docs column__allergy_id%}

Identifier for each allergy

{% enddocs %}

{% docs column__has_trace_of%}

If an ingredient has a trace of an allergy

{% enddocs %}

{% docs column__ingredient_name %}

The name of the ingredient connected to an ingredient id

{% enddocs %}

{% docs column__allergy_name %}

The name of an allergy connected to an allergy id

{% enddocs %}

{% docs column__recipe_step_id %}

The unique identifier for a recipe step in the source system

{% enddocs %}

{% docs column__recipe_step_description %}

The description of a recipe step, i.e. "Boil the pasta"

{% enddocs %}

---------
{% docs column__ingredient_nutrient_fact_id %}

The identifier for an ingredient's nutritional fact in the source system.

Values:
* `2`: Fat
* `3`: Protein
* `5`: Carbohydrates
* `6`: Salt
* `7`: Energy (kJ)
* `8`: Fat (saturated)
* `9`: Carbohydrates (sugar)
* `10`: Fiber
* `11`: Energy (kcal)
* `12`: Fresh fruit and vegetables
* `13`: Processed fruit and vegetables
* `14`: Added sugars
* `15`: Added salt

{% enddocs %}

{% docs column__ingredient_nutritional_value %}

The nutritional value of an ingredient's nutritional fact id per 100g of ingredient.

{% enddocs %}

