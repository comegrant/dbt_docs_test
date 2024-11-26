# Common

# Weekly Menus
{% docs column__weekly_menu_id %}

The primary key of a menu week in the pim database.

{% enddocs %}


# Menus
{% docs column__menu_id %}

The primary key of a menu in the pim database. 

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

The primary key of a recipe in the pim database.

{% enddocs %}

{% docs column__main_recipe_id %}

The id of the main recipe that belongs to the recipe. Is null if recipe is a main recipe.

{% enddocs %}

# Recipe Metadata
{% docs column__recipe_metadata_id %}

The primary key of recipe metadata table in the pim database.

{% enddocs %}

{% docs column__cooking_time_from%}

Minimum estimated time to make the recipe.

{% enddocs %}

{% docs column__cooking_time_to%}

Maximum estimated time to make the recipe.

{% enddocs %}

{% docs column__recipe_photo%}

Photo of the recipe, used by frontend in web. Append string as suffix to the URL: https://pimimages.azureedge.net/images/resized/ to visualize images in i.e. a streamlit app.

{% enddocs %}

# Recipe Portions
{% docs column__recipe_portion_id %}

...

{% enddocs %}

# Portions
{% docs column__portion_id %}

...

{% enddocs %}

{% docs column__portion_size %}

...

{% enddocs %}

# Recipe Ratings
{% docs column__recipe_rating %}

Rating (1-5) of the recipe given by the customer. If rating is 0 it means that the customer marked the recipe as not cooked.

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

{% docs column__recipe_main_ingredient_id%}

Identifier for the recipe's main ingredient

{% enddocs %}

{% docs column__recipe_difficulty_level_id%}

Identifier for the recipes difficulty level

{% enddocs %}

## Undocumented fields











