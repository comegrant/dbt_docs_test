# ML recipes
{% docs column__taxonomy_list %}

Aggregated list of taxonomy names for a recipe id.

{% enddocs %}

{% docs column__number_of_taxonomies %}

Number of taxonomies for a recipe id.

{% enddocs %}

{% docs column__generic_ingredient_id_list %}

Aggregated list of generic ingredient ids for a recipe id.

{% enddocs %}

{% docs column__number_of_ingredients %}

Number of ingredients for a recipe id.

{% enddocs %}

{% docs column__recipe_step_id_list %}

Aggregated list of recipe step ids for a recipe id.

{% enddocs %}

{% docs column__number_of_recipe_steps %}

Number of recipe steps for a recipe id.

{% enddocs %}

{% docs column__pk_estimations_log %}

The unique primary key in estimations_log table

{% enddocs %}

{% docs column__source_estimations_log %}

Indicates if the data is imported from ADB to get history, or if it is generated in the new data platform (NDP)

{% enddocs %}

{% docs column__product_variation_quantity_order_history %}

Aggregated quantity of a product variation id per company and menu week

{% enddocs %}

{% docs column__total_weekly_qty %}

Aggregated quantity of all product variation ids per company and menu week

{% enddocs %}

{% docs column__variation_ratio %}

Ratio of a product variation quantity to the total weekly product variationquantity for a company

{% enddocs %}

{% docs column__number_generic_ingredients %}

Number of generic ingredients for a recipe portion id

{% enddocs %}

{% docs column__generic_ingredient_id_list_per_recipe_portion %}

Aggregated list of generic ingredient ids for a recipe portion id

{% enddocs %}

{% docs column__generic_ingredient_name_list_per_recipe_portion %}

Aggregated list of generic ingredient names for a recipe portion id

{% enddocs %}

{% docs column__ingredient_id_list_per_recipe_portion %}

Aggregated list of ingredient ids for a recipe portion id

{% enddocs %}

{% docs column__ingredient_category_id_list_per_recipe_portion %}

Aggregated list of ingredient category ids for a recipe portion id

{% enddocs %}

{% docs column__most_recent_menu_year_week_main_recipe %}

Most recent menu year, week for a main recipe id and per billing agreement, company (YYYYWW)

{% enddocs %}

{% docs column__protein_gram_per_portion %}

Protein per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__carbs_gram_per_portion %}

Carbohydrates per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__fat_gram_per_portion %}

Fat per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__sat_fat_gram_per_portion %}

Saturated fat per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__sugar_gram_per_portion %}

Sugar per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__sugar_added_gram_per_portion %}

Added sugar per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__fiber_gram_per_portion %}

Fiber per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__salt_gram_per_portion %}

Salt per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__salt_added_gram_per_portion %}

Added salt per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__fg_fresh_gram_per_portion %}

Fresh fruit and vegetables per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__fg_proc_gram_per_portion %}

Processed fruit and vegetables per portion in grams for a recipe portion id.

{% enddocs %}

{% docs column__total_kcal_per_portion %}

Total calories per portion for a recipe portion id.

{% enddocs %}

{% docs column__is_low_calorie %}

Whether or not the recipe is low calorie. A recipe is considered low calorie if the total calories per portion is less than the threshold for the country (NO: 750, SE: 550, DK: 600), and the sum of fresh and processed fruit and vegetables per portion is greater than 150 grams.

{% enddocs %}

{% docs column__is_high_fiber %}

Whether or not the recipe is high fiber. A recipe is considered high fiber if the fiber per portion is greater than 10 grams.

{% enddocs %}

{% docs column__is_low_fat %}

Whether or not the recipe is low fat. A recipe is considered low fat if the fat per portion is less than 30% of the total calories per portion.

{% enddocs %}

{% docs column__is_low_sugar %}

Whether or not the recipe is low sugar. A recipe is considered low sugar if the sugar per portion is less than 7% of the total calories per portion.

{% enddocs %}

{% docs column__menu_feedback_model__column__number_of_users %}

Number of users who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__negative_taste_preference_combo_id %}

Unique identifier for a company and negative taste preferences combination

{% enddocs %}

{% docs column__taxonomy_id_list %}

Aggregated list of taxonomy ids for a recipe id.

{% enddocs %}

{% docs column__cumulated_times_on_menu %}

Cumulated times a recipe id has been on the menu based the main recipe id

{% enddocs %}

{% docs column__cumulated_number_of_ratings %}

Cumulated number of rating a recipe id has received based on the main recipe id

{% enddocs %}

{% docs column__cumulated_average_rating %}

Cumulated average rating of a recipe id based on the main recipe id

{% enddocs %}

{% docs column__users_with_1_portions %}

Number of active users with portion size 1 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__users_with_2_portions %}

Number of active users with portion size 2 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__users_with_3_portions %}

Number of active users with portion size 3 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__users_with_4_portions %}

Number of active users with portion size 4 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__users_with_5_portions %}

Number of active users with portion size 5 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__users_with_6_portions %}

Number of active users with portion size 6 who have the same negative taste preferences for a company

{% enddocs %}

{% docs column__ingredient_id_list %}

Aggregated list of ingredient ids for a recipe id.

{% enddocs %}

# Tofu Order History

{% docs column__total_orders %}

Total number of orders per menu year, week, and company.

{% enddocs %}

{% docs column__total_orders_with_flex %}

Number of orders of that contains flex: includijng swaps, additions, or removals of default dishes. The calculation method differs by time period:
- Pre-OneSub (≤ week 202446): Count of orders of legacy financial product type
- Post-OneSub (≥ week 202447): Count of distinct orders with has_swap, is_removed_dish, or is_added_dish flags

{% enddocs %}

{% docs column__flex_share %}

Percentage of orders that include flex, calculated as total_orders_with_flex / total_orders. This metric helps track adoption of menu customization features over time.

{% enddocs %}


# Tofu Latest Forecasts

{% docs column__forecast_total_orders %}

Latest forecasted number of total orders for a  menu week and company.

{% enddocs %}

{% docs column__forecast_flex_orders %}

Latest forecast of the number of orders that includes one or more flex dishes (swaps, additions, or removals) for a menu week and company.

{% enddocs %}

{% docs column__forecast_flex_share %}

Latest forecast of the percentage of orders that includes one or more flex dishes (swaps, additions, or removals) for a menu week and company.

{% enddocs %}


# Reci-pick Models

{% docs column__cooking_time_mean %}

Average cooking time in minutes, calculated as the mean of cooking_time_from and cooking_time_to.

{% enddocs %}


{% docs column__has_chefs_favorite_taxonomy %}

Whether or not the recipe has the chefs favorite taxonomy. Created based on keywords matching the taxonomy name. Keywords include inspiration, inspirerende, favoritter, chefs choice, cockens val, inspirerande.

{% enddocs %}

{% docs column__has_quick_and_easy_taxonomy %}

Whether or not the recipe has the quick and easy taxonomy. Created based on keywords matching the taxonomy name. Keywords include express, rask, laget på 1-2-3, fort gjort, snabb, enkelt, hurtig, nem på 5.

{% enddocs %}

{% docs column__has_vegetarian_taxonomy %}

Whether or not the recipe has the vegetarian taxonomy. Created based on keywords matching the taxonomy name. Keywords include vegetarian, vegan.

{% enddocs %}

{% docs column__has_low_calorie_taxonomy %}

Whether or not the recipe has the low calorie taxonomy. Created based on keywords matching the taxonomy name. Keywords include low calorie, sunn, sund, roede, kalorismart, viktväktarna, sund, kalorilet.

{% enddocs %}


{% docs column__menu_yyyyww %}

Menu year and week combined into a single integer.

{% enddocs %}
