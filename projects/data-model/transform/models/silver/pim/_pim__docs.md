# Recipe Difficulty Level Translations
{% docs column__recipe_difficulty_name %}

...

{% enddocs %}

# Chef Ingredients
{% docs column__chef_ingredient_id %}

Identifier for chef ingredient, which is an instance of generic ingredient with a 1-1 mapping with order ingredient.

{% enddocs %}

{% docs column__chef_ingredient_section_id %}

Identifier for each ingredient section, where an ingredient section is i.e. "pan-fried vegetables" contains the ingredient "carrots" and "broccoli"

{% enddocs %}

{% docs column__recipe_ingredient_amount %}

Amount of specific ingredient in a recipe variation

{% enddocs %}

# Weekly Menus
{% docs column__weekly_menu_id %}

The primary key of a menu week in the pim database.

{% enddocs %}

{% docs column__ingredient_purchase_date %}

The set purchasing date for the ingredients to be supplied for a specific menu week.

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

The unique identifier of a recipes.

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

The creation timestamp of the recipe variation.

{% enddocs %}

{% docs column__source_created_by_recipes %}

The creator user name of the recipe variation.

{% enddocs %}

{% docs column__source_updated_at_recipes %}

The last updated timestamp of the recipe variation.

{% enddocs %}

{% docs column__source_updated_by_recipes %}

The user who made the last update to the recipe variation.


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

The status code of the recipe_id. Connects to the pim status codes table.

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

{% docs column__ingredient_id %} 

Identifier for an ingredient.

{% enddocs %}

{% docs column__ingredient_type_id %}

Parameter for production-line placement of products

{% enddocs %}

{% docs column__ingredient_content_list %}

Description of an ingredient's composition, e.g. "oil, egg, salt" for "mayonnaise"

{% enddocs %}

{% docs column__ingredient_net_weight %}

The net weight of an ingredient.

{% enddocs %}

{% docs column__ingredient_internal_reference %}

An alternative unique ingredient id number which is sometimes used as link between tables. Typically starts with either GL, bhub or basis with or without '-' followed by a number.

{% enddocs %}

{% docs column__pack_type_id %}

Id of the pack type, unique identifier of the pack type table.

{% enddocs %}

{% docs column__epd_id_number %}

Id number that serves as a link to external product information database.

{% enddocs %}

{% docs column__ingredient_manufacturer_supplier_id %}

Id number of the manufacturer of the ingredient. Links to the ingredient_suppliers table 

{% enddocs %}

{% docs column__has_customer_photo %}

Indicates if there is a linked photo of the ingredient that is displayed for customers.

{% enddocs %}

{% docs column__has_internal_photo %}

Indicates if there is a linked photo of the ingredient for internal use.

{% enddocs %}

{% docs column__has_packaging_photo %}

Indicates if there is a packaging photo linked.

{% enddocs %}

{% docs column__is_active_ingredient %}

Indicates if the ingredient is active.

{% enddocs %}

{% docs column__is_available_for_use %}

Indicates if the ingredient is available for use in menu planning.

{% enddocs %}

{% docs column__is_cold_storage %}

Indicates if the ingredient should handled in cold storage in production.

{% enddocs %}

{% docs column__is_consumer_cold_storage %}

Indicates if the ingredient is to be stored cold by the customer/consumer.

{% enddocs %}

{% docs column__is_fragile_ingredient %}

Indicates if the ingredient is fragile and should be handled carefully in production.

{% enddocs %}

{% docs column__is_organic_ingredient %}

Indicates if the ingredient is classified as organic.

{% enddocs %}

{% docs column__is_outgoing_ingredient %}

Indicates if the ingredient is soon not to be used in menu planning.

{% enddocs %}

{% docs column__is_special_packing %}

Indicates if the ingredient is a special packaging ingredient.

{% enddocs %}

{% docs column__is_to_register_batch_number %}

Indicates if the batch number should be registered upon reception at the production site.

{% enddocs %}

{% docs column__is_to_register_expiration_date %}

Indicates if the expiration date should be registered upon reception at the production site.

{% enddocs %}

{% docs column__is_to_register_ingredient_weight %}

Indicates if the ingredient weight should be registered upon reception at the production site.

{% enddocs %}

{% docs column__is_to_register_temperature %}

Indicates if the temperature should be registered upon reception at the production site.

{% enddocs %}

{% docs column__ingredient_gross_weight %}

The gross weight of the ingredient, i.e. including packaging.

{% enddocs %}

{% docs column__ingredient_shelf_life %}

The maximum number of days until "use by" date is due.

{% enddocs %}

{% docs column__distribution_packages_per_pallet %}

The number of distribution packages of the ingredient that are packed on a pallet.

{% enddocs %}

{% docs column__ingredient_distribution_packaging_size %}

The number of consumer packages that are contained in one distribution package.

{% enddocs %}

{% docs column__ingredient_packaging_depth %}

The depth in mm of the ingredient consumer packaging.

{% enddocs %}

{% docs column__ingredient_packaging_height %}

The height in mm of the ingredient consumer packaging.

{% enddocs %}

{% docs column__ingredient_packaging_width %}

The width in mm of the ingredient consumer packaging.

{% enddocs %}

{% docs column__ingredient_size %}

The amount contained in the ingredient package, in the unit specified by the unit_label_id

{% enddocs %}

{% docs column__ingredient_co2_emissions_per_kg %}

The number of kilos CO2 emissions registered per one kilo of the ingredient.
Note: Not all ingredients has registered CO2 emissions.

{% enddocs %}

{% docs column__ingredient_co2_emissions_per_unit %}

The registered CO2 emission (in kg) for one unit of the ingredient. It is calculated by multipling the emission per kg of the ingredient with the net weight of one unit of the ingredient. 
Note: Not all ingredients have registered CO2 emission data. 

{% enddocs %}

{% docs column__has_co2_data %}

A flag stating if the ingredient has registered data about it's CO2 emission.

{% enddocs %}

{% docs column__ean_code_consumer_packaging %}

The EAN code for the consumer packaging.

{% enddocs %}

{% docs column__ean_code_distribution_packaging %}

The EAN code for the distribution packaging.

{% enddocs %}

{% docs column__ingredient_brand %}

The brand of the ingredient.

{% enddocs %}

{% docs column__ingredient_external_reference %}

A reference number to the article number used by the supplier.

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

Identifier for the status.

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

# Weekly Orders

{% docs column__purchase_order_id %}

Unique identifier of weekly purchase orders in cms.

{% enddocs %}

{% docs column__purchase_order_status_id %}

The status code of the purchase order.

{% enddocs %}

{% docs column__is_special_purchase_order %}

True if the purchase order has been made outside of the normal routine, i.e. to purchase something outside of the recipes or buy extra quantity.

{% enddocs %}

{% docs column__production_date %}

The planned production date in our production facility for the ingredients on the purchase order.

{% enddocs %}

{% docs column__purchase_delivery_date %}

When the ingredients on the purchase order will be delivered to our production facility.

{% enddocs %}

{% docs column__purchase_order_date %}

When purchase order was done.

{% enddocs %}

# Weekly Order Lines

{% docs column__purchase_order_line_id %}

The unique identifier for purchase order lines in cms.

{% enddocs %}

{% docs column__purchase_order_line_status_id %}

Id of status of purchase order. Status can be found in procurement_status in pim. Statuses include:
* 10 - WIP
* 20 - Approved
* 30 - Draft
* 40 - Ordered
* 50 - Pre-order

{% enddocs %}

{% docs column__original_ingredient_quantity %}

The ingredient quantity to order, based on the need from the recipes and the customer orders.

{% enddocs %}

{% docs column__ingredient_purchasing_cost %}

The cost of the ingredient on the purchase order line.

{% enddocs %}

{% docs column__total_purchasing_cost %}

Total cost of the order line. Ingredient cost * Total ingredient quantity.

{% enddocs %}

{% docs column__extra_ingredient_quantity %}

Any extra ingredient quantity to order based on assumed waste or distribution pack sizes, or corrections to the original_ingredient_quantity.

{% enddocs %}

{% docs column__total_ingredient_quantity %}

Total ingredient quantity on the order line, i.e. original_ingredient_quantity + extra_ingredient_quantity. Does not include take_from_storage_ingredient_quantity.

{% enddocs %}

{% docs column__take_from_storage_ingredient_quantity %}

The ingredient quantity to be taken from internal storage, and not from external supplier.

{% enddocs %}

{% docs column__received_ingredient_quantity %}

The actual ingredient quantity registered as received in production.

{% enddocs %}

# Ingredient Prices

{% docs column__ingredient_price_id %}

This is the primary key of the ingredient_price table in PIM.

{% enddocs %}

{% docs column__ingredient_price_type_id %}

Identifier of the type of price:
* 0: Regular
* 1: Campaign
* 2: Marketing Contribution

{% enddocs %}

{% docs column__ingredient_unit_cost %}

The planned cost of purchasing ingredients. Used for procurement planning.

{% enddocs %}

{% docs column__ingredient_unit_cost_markup %}

Unit price with an extra mark up used to reduce the risk of price increases when planning menus.

{% enddocs %}

{% docs column__ingredient_price_valid_from %}

Date from which the price of the ingredient is valid.

{% enddocs %}

{% docs column__ingredient_price_valid_to %}

Date until which the price of the ingredient is valid.

{% enddocs %}

# Price Categories

{% docs column__price_category_level_id %}

The level id of the price category.

{% enddocs %}

{% docs column__price_category_min_total_ingredient_cost %}

The lowest recipe cost (in whole units) for a recipe to fit in to the price category.

{% enddocs %}

{% docs column__price_category_max_total_ingredient_cost %}

The highest recipe cost (in whole units) for a recipe to fit into the price category.

{% enddocs %}

{% docs column__price_category_price_inc_vat %}

The additional price to be charged from the customer for a recipe in the price category. The price can also be negative for thrifty dishes.

{% enddocs %}

{% docs column__price_category_level_name %}

The name of the price category level.

{% enddocs %}

{% docs column__price_category_valid_from %}

The date from which the price category row is valid.

{% enddocs %}

{% docs column__price_category_valid_to %}

The date until which the price category row is valid.

{% enddocs %}

# Order Ingredients
{% docs column__order_ingredient_id %}

Identifier for an ingredient connected to a specific recipe variation

{% enddocs %}

{% docs column__is_main_protein %}

If an ingredient is used as the main protein in a recipe

{% enddocs %}

{% docs column__is_main_carbohydrate %}

If an ingredient is used as the main carbohydrate in a recipe

{% enddocs %}

{% docs column__nutrition_units %}

The amount of an ingredient which is included in nutritional calculations. The unit is specified by the unit label.

{% enddocs %}

{% docs column__ingredient_order_quantity %}

The amount of an ingredient to be ordered for a recipe. The unit is specified by the unit label.

{% enddocs %}

# Procurement Cycles

{% docs column__purchasing_company_id %}

Id of the company used for purchasing ingredients. Eg. in Norway where there are multiple brands but they order ingredients as the same company_id, which is different from the brand company_id.

{% enddocs %}

{% docs column__distribution_center_id %}

Id of the distribution center where the ingredients are delivered to.

{% enddocs %}

# Suppliers
{% docs column__ingredient_supplier_id %} 

Identifier for each ingredient supplier 

{% enddocs %}

{% docs column__ingredient_supplier_status_code_id %} 

The status id of the supplier. Connects to the pim status codes table.

{% enddocs %}

{% docs column__ingredient_supplier_name %} 

The name of the ingredient supplier.

{% enddocs %}

# Unit Labels
{% docs column__unit_label_id %} 

Unique identifier for each unit label.

{% enddocs %}

{% docs column__unit_label_status_code_id %} 

The status code of the unit label. Connects to the pim status codes table. 

{% enddocs %}

{% docs column__unit_label_short_name %} 

The unit label name in short form in local language. 

{% enddocs %}

{% docs column__unit_label_short_name_plural %} 

The unit label name in short form, plural, in local language.

{% enddocs %}

{% docs column__unit_label_full_name %} 

The full unit label name in local language.

{% enddocs %}

# Weekly Ingredients
{% docs column__weekly_ingredient_id %} 

Unique identifier for each row in the table.

{% enddocs %}

{% docs column__weekly_ingredient_quantity %} 

The ingredient quantity for each of the menu variations of that menu week

{% enddocs %}

{% docs column__is_fetched_from_recipes %} 

Indicates if the ingredient quantity is derived from a recipe or not.

{% enddocs %}

{% docs column__is_active_weekly_ingredient %} 

Indicates if the weekly ingredient rows is active or not. If not active they will not be sent to purchasing or production, but will still be in the recipe.

{% enddocs %}

# Not Organized
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

{% docs column__nutrition_calculation %}

Indicates if the ingredient has nutrition calculation

{% enddocs %}

{% docs column__ingredient_nutrient_fact_name %}

The name of the ingredient's nutritional fact id, e.g. "Fat"

{% enddocs %}
