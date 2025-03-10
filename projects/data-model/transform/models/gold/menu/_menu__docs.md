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

...

{% enddocs %}

{% docs column__has_menu_recipes %}

...

{% enddocs %}

{% docs column__has_recipe_portions %}

...

{% enddocs %}

{% docs column__is_dish %}

...

{% enddocs %}

{% docs column__is_artificial_week %}

...

{% enddocs %}

{% docs column__portion_quantity %}

...

{% enddocs %}

{% docs column__portion_id_menu_variations %}

portion_id obtained from the menu variations.

{% enddocs %}

{% docs column__portion_id_recipes %}

portion_id obtained from the recipes.

{% enddocs %}

{% docs column__portion_id_products %}

portion_id obtained from product variations.

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

# Dim Products
{% docs column__pk_dim_products %}

Primary key of dim_products. It is a composite of product_variation_id and company_id.

{% enddocs %}

{% docs column__meals %}

Number of meals in the product (e.g. in the mealbox). One can also have information about meals for other product types, like Velg&Vrak and Standalone-Groceries. However, for these the meals is usually 1.

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


{% docs  column__main_group %}

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

{% docs column__fk_dim_ingredients %}

Foreign key to dim_ingredients

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
