# Weekly Menus
{% docs column__weekly_menu_id %}

...

{% enddocs %}

{% docs column__weekly_menu_status_code_id %}

...

{% enddocs %}

{% docs column__menu_week %}

...

{% enddocs %}

{% docs column__menu_year %}

...

{% enddocs %}

{% docs column__menu_week_monday_date %}

...

{% enddocs %}

# Menus

{% docs column__menu_id %}

...

{% enddocs %}

{% docs column__menu_status_code_id %}

...

{% enddocs %}

# Menu Variations

{% docs column__menu_variation_margin_id %}

...

{% enddocs %}

{% docs column__portion_id %}

...

{% enddocs %}

{% docs column__menu_number_days %}

...

{% enddocs %}

{% docs column__menu_price %}

...

{% enddocs %}

{% docs column__menu_cost %}

...

{% enddocs %}

# PIM Old Docs

## Tables


{% docs table__weekly_menus %}

This table holds information about each menu week (delivery week). There is one row per company per week of the year. 

{% enddocs %}

{% docs table__menus %}

This table holds all menus per weekly menu.

{% enddocs %}

{% docs table__menu_variations %}

This table links menus to variations. Hence there is one row per menu_id and menu_variation_ext_id (which is equvivalent to the product_variation_id).

{% enddocs %}

{% docs table__menu_recipes %}

...

{% enddocs %}

{% docs table__recipes %}

...

{% enddocs %}

{% docs table__recipe_portions %}

...

{% enddocs %}

{% docs table__portions %}

...

{% enddocs %}

## Fields

{% docs column__ingredient_category_id %} Identifier for each ingredient category {% enddocs %}

{% docs column__ingredient_category_name %} Name of ingredient category {% enddocs %}

{% docs column__parent_category_id %} Identifier for the ingredient category parent, indicating the hierarchy within ingredient categories. If null, the ingredient category is at the top of the hierarchy. {% enddocs %}

{% docs column__taxonomy_id %} Identifier for each taxonomy {% enddocs %}

{% docs column__taxonomy_name %} Name of the taxonomy {% enddocs %}

{% docs column__generic_ingredient_id %} Identifier for each generic ingredient, i.e. "soy-sauce"  {% enddocs %}

{% docs column__generic_ingredient_name %} Name of the generic ingredient {% enddocs %}

{% docs column__ingredient_content %} Description of an ingredient's composition, e.g. "oil, egg, salt" for "mayonnaise" {% enddocs %}

{% docs column__is_external_taxonomy %} If the taxonomy is used in frontend, i.e. if it is a user friendly taxonomy {% enddocs %}

{% docs column__status_code_id %}

Identifier for the status of a certain object.

* `1`: Enabled
* `2`: Disabled
* `3`: Published
* `4`: Draft
* `5`: Changed
* `6`: Locked

{% enddocs %}

{% docs column__ingredient_status_code_id %} Identifier for the status of an ingredient. Related to the status_code_id {% enddocs %}

{% docs column__is_main_protein %} If an ingredient is used as the main protein in a recipe {% enddocs %}

{% docs column__is_main_carbohydrate %} If an ingredient is used as the main carbohydrate in a recipe {% enddocs %}

{% docs column__supplier_id %} Identifier for each ingredient supplier {% enddocs %}

{% docs column__recipe_id %} Identifier for a recipe {% enddocs %}

{% docs column__recipe_portion_id %} Identifier for a specific portion variation of a recipe {% enddocs %}

{% docs column__ingredient_amount %} Amount of specific ingredient in a recipe variation {% enddocs %}

{% docs column__order_ingredient_id %} Identifier for an ingredient connected to a specific recipe variation {% enddocs %}

{% docs column__ingredient_id %} Identifier for an ingredient (connected to a supplier) {% enddocs %}

{% docs column__ingredient_internal_reference %} Used to join ingredient ids across different databases {% enddocs %}

## Undocumented fields

{% docs column__chef_ingredient_section_id %}  {% enddocs %}

{% docs column__chef_ingredient_id %}  {% enddocs %}

{% docs column__ingredient_type %}  {% enddocs %}




