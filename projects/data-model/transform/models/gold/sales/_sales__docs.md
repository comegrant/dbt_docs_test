# Fact Orders
{% docs column__pk_fact_orders %}

The unique key of each row in Fact Orders.

{% enddocs %}

{% docs column__weeks_since_first_order %}

Number of weeks since the first order of the billing agreement.

{% enddocs %}

{% docs column__preselected_product_variation_id %}

The product variation id of the dish presented to the customer on the webpage.

{% enddocs %}

{% docs column__preselected_recipe_id %}

The recipe id of the dish preseneted to the customer on the webpage.

{% enddocs %}

{% docs column__mealbox_servings %}

The total number of mealbox_servings in a mealbox. I.e., ordered portions * ordered meals. This is null for all order lines that are not a mealbox.

{% enddocs %}

{% docs column__has_delivery %}

1 if the order has delivery. E.g., gift card orders does not have a delivery, most other orders does.

{% enddocs %}

{% docs column__has_swap %}

True if the customer has swapped in or out dishes when placing the orders. Is false if they only removed or added one dish without replacing it with another recipe, or if the customer did not make any changes to the order at all.

{% enddocs %}

{% docs column__is_added_dish %}

Is 1 if the ordered dish was added by the customer and 0 if the ordered dish was preselected for the customer. For all rows that are not representing a dish the field will be null.

{% enddocs %}

{% docs column__is_removed_dish %}

Is 1 if the preselected dish was removed by the customer and 0 if the preselected dish was ordered. For all rows that are not representing a dish the field will be null.

{% enddocs %}

{% docs column__is_thrifty_dish %}

Customers can select dishes that give them a price decrease. Column is 1 if the customer was discounted for the dish on their order. 0 for all dishes that did not have a minus price. Null if the order line is not a dish.

{% enddocs %}

{% docs column__is_plus_price_dish %}

Customers can select dishes for an extra price. The column is 1 if the customer was charged extra for the dish on their order. 0 for all dishes that did not have an extra price. Null if the order line is not a dish.

{% enddocs %}

{% docs column__is_subscribed_grocery %}

True if the order line is a grocery the customer subscribes to, else false.

{% enddocs %}

{% docs column__is_onesub_migration %}

1 if the customers order was migrated to onesub. Meaning that the product on the order line was changed to the Onesub mealbox product.

{% enddocs %}

{% docs column__is_missing_preselector_output %}

1 if the customer did not see any preselector output in the frontend before selecting recipes, else 0.

{% enddocs %}

{% docs column__is_adjusted_by_customer %}

True if customers edited the order before placing it. This could be swapping recipes, adding portions, adding meals or adding groceries, else false.

{% enddocs %}

{% docs column__meal_adjustment_subscription %}

The difference between the ordered number of meals and the number of meals the customer subscribes to.

{% enddocs %}

{% docs column__portion_adjustment_subscription %}

The difference between the ordered number of portions and the number of portions the customer subscribes to.

{% enddocs %}

{% docs column__subscribed_product_variation_quantity %}

Quantity subscribed to of the product.

{% enddocs %}

{% docs column__subscribed_product_variation_amount_ex_vat %}

The total amount of the order line if only including subscribed product variation quantity. E.g. if a customer subscribed to one milk and adds two more. This column will only contain the amount coming from one milk.

{% enddocs %}

{% docs column__fk_dim_products_preselected %}

Used to fetch product information about the dishes that has been presented to the customer on the webpage before the order was placed. Is 0 for order lines that are not representing a preselected dish.

{% enddocs %}

{% docs column__fk_dim_recipes_preselected %}

Used to fetch recipe information about the dishes that has been presented to the customer on the webpage before the order was placed. Is 0 for order lines that are not representing a preselected dish.

{% enddocs %}

{% docs column__recipe_rating_id %}

Defines a unique recipe rating. To be used to count number of ratings. A concatenation of the billing_agreement_id and the recipe_id from the recipe ratings table.

{% enddocs %}

{% docs column__recipe_comment_id %}

Defines a unique recipe comment. To be used to count number of comments. A concatenation of the billing_agreement_id and the recipe_id from the recipe comments table.

{% enddocs %}

{% docs column__fk_dim_periods_since_first_menu_week %}

Foreign key to the dimension dim_periods_since, representing number of days since the first menu week of the agreement.

{% enddocs %}


# Dim Billing Agreements

{% docs column__pk_dim_billing_agreements %}

The unique id of each row in Dim Billing Agreements

{% enddocs %}

{% docs column__first_menu_week_monday_date %}

The weeks monday date of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_week %}

The week of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_month %}

The month name of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_month_number %}

The month number of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_quarter %}

The quarter of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_year %}

The year of the customers first delivery.

{% enddocs %}

{% docs column__has_grocery_subscription %}

True if the customer has groceries in their subscription, else false.

{% enddocs %}

{% docs column__onesub_flag%}

Describes if the customer has a OneSub product or not in their basket. It will not "OneSub" from the time when the customer got OneSub in their basket for each individual customer and "Not OneSub" before.

{% enddocs %}

{% docs column__preselector_flag %}

Describes if a customer has been rolled over to the preselector during Onesub launch. The field is changed to "Preselector" at the time preselector was run first time for each individual customer, and is "Not Preselector" before this for each customer. After the launch people get preselector output for the weeks visible in the webpage if they have taken the preference quiz and not made a deviation themselves. For the weeks not visible in the webpage at launch the preselector will run for all customers.

{% enddocs %}

{% docs column__onesub_beta_flag %}

Describes if the customer was a part of the Internal Launch or the 10% Customer Beta Launch of Onesub.

{% enddocs %}


# Dim Order Statuses

{% docs column__pk_dim_order_statuses %}

The unique id of the rows in Dim Order Statuses

{% enddocs %}

# Dim Order Types

{% docs column__pk_dim_order_types %}

The unique id of the rows in Dim Order Types

{% enddocs %}

# Dim Order Line Details

{% docs column__pk_dim_order_line_details %}

The unique id of the rows in Dim Order Line Details

{% enddocs %}

{% docs column__order_line_details %}

Detials on the type of order line. Such as plus price dish, thrifty dish, groceries, mealbox etc.

{% enddocs %}

# Dim Preferences

{% docs column__pk_dim_preferences %}

Primary key of the preferences dimension. It is a composite key of preference_id and company_id.

{% enddocs %}

# Dim Preference Combinations

{% docs column__pk_dim_preference_combinations %}

Primary key of the preference combination dimension. It is the same as the primary key for the billing agreement dimension.

{% enddocs %}


{% docs column__preference_combinations %}

A list of all preference and attribute names belonging to a customer. The names in the list are general across the companies.

{% enddocs %}

{% docs column__preference_id_combinations %}

A list of all preference ids belonging to a customer.

{% enddocs %}

{% docs column__concept_combinations %}

A list of concept preference names belonging to a customer. The names in the list are general across the companies.

{% enddocs %}

{% docs column__preference_id_combinations_concept_type %}

A list of concept preference ids belonging to a customer.

{% enddocs %}

{% docs column__taste_preference_combinations %}

A list of taste preference names belonging to a customer. The names in the list are general across the companies.

{% enddocs %}

{% docs column__preference_id_combinations_taste_type %}

A list of taste preference ids belonging to a customer.

{% enddocs %}

{% docs column__number_of_preferences %}

The number of preferences of all types in the preference combination.

{% enddocs %}

{% docs column__number_of_concept_preferences %}

The number of concept preferences in the preference combination.

{% enddocs %}

{% docs column__number_of_taste_preferences %}

The number of taste preferences in the preference combination.

{% enddocs %}

# Bridge Billing Agreements Preferences

{% docs column__pk_bridge_billing_agreements_preferences %}

...

{% enddocs %}

{% docs column__billing_agreement_preferences_updated_id %}

...

{% enddocs %}

# Fact Subscription Quiz

{% docs column__pk_fact_subscription_quiz %}

Primary key of the fact subscription quiz table. It is a composite key of segment_event_id, segment_updated_at, billing_agreement_id, company_id, has_started_subscription_quiz and has_completed_subscription_quiz.

{% enddocs %}

{% docs column__fk_dim_date_source_created_at_segment %}

Foreign key that is used to relate the created at timestamp from Segment to Dim Date.

{% enddocs %}

{% docs column__fk_dim_time_source_created_at_segment %}

Foreign key that is used to relate the created at timestamp from Segment to Dim Time.

{% enddocs %}

{% docs column__has_started_subscription_quiz %}

The customer has started the subscription quiz to update their subscription preferences, size, and portions.

{% enddocs %}

{% docs column__has_completed_subscription_quiz %}

The customer has completed the subscription quiz to update their subscription preferences, size, and portions.

{% enddocs %}

# Dim Preselector Versions

{% docs column__pk_dim_preselector_versions %}

The unique id of the rows in Dim Preselector Versions.

{% enddocs %}

{% docs column__model_version_short_commit_sha %}

The short commit sha for the model version that was used to generate the output.

{% enddocs %}

{% docs column__source_model_version_first_used_at %}

The first time the model version was used to generate the output.

{% enddocs %}

{% docs column__source_model_version_latest_used_at %}

The latest time the model version was used to generate the output.

{% enddocs %}

{% docs column__preselector_version_number %}

An arbitrary version number that is assigned to that commit_sha. Calculated using the row number when sorting by the source_model_version_first_used_at column in ascending order. It is therefore possible that the version numbers don't match up exactly with all commit_shas in the preselector project, especially if there are multiple commits in a single day.

{% enddocs %}

# Fact Preselector

{% docs column__pk_fact_preselector %}

The unique id of each row in Fact Preselector.

{% enddocs %}

{% docs column__fk_dim_dates_menu_week_monday_date %}

Foreign key that is used to relate the monday date of the menu week to Dim Dates.

{% enddocs %}

{% docs column__fk_dim_dates_created_at_preselector_output %}

Foreign key that is used to relate the created at date from the preselector output to Dim Date.

{% enddocs %}

{% docs column__fk_dim_time_created_at_preselector_output %}

Foreign key that is used to relate the created at time from the preselector output to Dim Time.

{% enddocs %}

{% docs column__repeat_weeks %}

Counts how many times the main recipe which was output by the preselector has been selected for a customer in:
- The previous 6 menu weeks
- Any currently visible menu weeks that haven't been ordered yet

This helps identify when the same recipe is being repeatedly selected ("repeat selection"). Repeat selections are undesirable since customers prefer variety in their meals.

{% enddocs %}

{% docs column__menu_week_window %}

The number of menu weeks that we want to compare the preselector output against can vary depending on what the week the preselector was output for.

We always compare the preselector output against the previous 6 menu weeks and the current future menu weeks visible on the front-end, but the number of future weeks visible in the front-end can vary.

E.g. if the preselector output was for the next visible menu week in the front-end, we will compare it against the previous 6 weeks ordered weeks and the other 3 weeks on the front-end, resulting in the number of weeks in the window being 9. But if the preselector output was for the furthest away week in the front-end, we will compare it against the previous 6 weeks, however there are no other future weeks visible on the front-end, resulting in the number of weeks in the window being 6

{% enddocs %}

{% docs column__repeat_weeks_percentage %}

The percentage of weeks in the previous 6 menu weeks, and future visible menu weeks, where this main recipe id was also selected.

The reason for this calculation is that the number of weeks in the comparison window can vary, so we need to normalise the number of repeats with the number of weeks in the window.

{% enddocs %}

{% docs column__dish_rotation_score %}

A score from 0 to 1 which indicates how often a dish has been repeated in recent weeks. The closer to 1, the less the dish has been repeating in recent weeks. Calculated as 1 - repeat_weeks_percentage.

{% enddocs %}

{% docs column__rotation_score %}

A score from 0 to 1 of which indicates how often the dishes have been repeated in recent weeks. The closer to 1, the "newer" dishes are to the customer, the closer to 0 the more the dishes have been repeated recently.

If you are looking at a dish then this is calculated as 1 - repeat_weeks_percentage, if you are looking at a mealbox, then it's the average of the dishes in the mealbox, calculated as 1 - avg(repeat_weeks_percentage).

{% enddocs %}

{% docs column__rotation_score_group %}

A grouping of rotation_score to provide a guide on what is good and what is not, broken down into the groups "Very High", "High", "Medium", "Low", "Very Low"

{% enddocs %}

{% docs column__rotation_score_group_number %}

A grouping of rotation_score in number format to provide a guide on what is good and what is not, broken down into the groups 1 (Very High),2,3,4,5 (Very Low)

{% enddocs %}

{% docs column__number_of_unique_main_ingredients %}

The number of unique main ingredients within a mealbox output by the preselector

{% enddocs %}

{% docs column__main_ingredient_variation_score %}

A score from 0 to 1 which indicates how varied the main ingredients are in a mealbox output by the preselector. Calculated as number_of_unique_main_ingredients divided by the number of meals

{% enddocs %}

{% docs column__main_ingredient_variation_score_group %}

A grouping of protein_variation_score to provide a guide on what is good and what is not, broken down into the groups "Very High", "High", "Medium", "Low", "Very Low"

{% enddocs %}

{% docs column__main_ingredient_variation_score_group_number %}

A grouping of protein_variation_score in number format to provide a guide on what is good and what is not, broken down into the groups 1 (Very High),2,3,4,5 (Very Low)

{% enddocs %}

{% docs column__is_dish_preselector_output %}

A boolean which indicates whether the row relates to a single dish or the whole mealbox. If true the it's the individual dishes, if false then it's the mealbox

{% enddocs %}

{% docs column__main_recipe_ids %}

The list of all main_recipe_ids that are in the mealbox for that preselector output

{% enddocs %}

{% docs column__combined_rotation_variation_score %}

A measure of the overall quality of the selection in the mealbox we gave the customer. It's calculated as the main_ingredient_variation_score * rotation_score for the mealbox and goes from 0 to 1, where 1 is good and 0 is bad.

{% enddocs %}

{% docs column__combined_rotation_variation_score_group %}

A grouping of combined_rotation_variation_score to provide a guide on what is good and what is not, broken down into the groups "Very High", "High", "Medium", "Low", "Very Low"

{% enddocs %}

{% docs column__combined_rotation_variation_score_group_number %}

A grouping of combined_rotation_variation_score in number format to provide a guide on what is good and what is not, broken down into the groups 1 (Very High),2,3,4,5 (Very Low)

{% enddocs %}

{% docs column__sum_error_main_ingredients %}

The sum of all preselector errors that relate to the choice of proteins or main ingredients in a mealbox

{% enddocs %}

# Fact Billing Agreement Consents

{% docs column__pk_fact_billing_agreement_consents %}

The unique key of each row in Fact Billing Agreement Consents.

{% enddocs %}

# Dim Consents

{% docs column__pk_dim_consent_types %}

The unique key of each row in Dim Consents.

{% enddocs %}

# Fact Billing Agreement Updates

{% docs column__pk_fact_billing_agreement_updates %}

The unique key of each row in Fact Billing Agreement Updates. This is the same as the foreign key to the updated billing agreement.

{% enddocs %}

{% docs column__fk_dim_dates_first_menu_week%}

Foreign key to dim_dates representing the monday date of the menu week each agreement had their first delivery.

{% enddocs %}


{% docs column__fk_dim_billing_agreements_updated %}

The primary key of Dim Billing Agreements. The key is representing the version of the billing agreement after the update.

{% enddocs %}

{% docs column__fk_dim_billing_agreements_previous_version %}

The primary key of Dim Billing Agreements. The key is representing the version of the billing agreement before the update.

{% enddocs %}

{% docs column__updated_at %}

The timestamp of when the update of the billing agreement happened.

{% enddocs %}

{% docs column__is_signup %}

True if this is the first version of the agreement which has status, subscribed product, preferences and loyalty level.

{% enddocs %}

{% docs column__has_first_delivery %}

True if this version of the agreement was valid at the time the customer had their first delivery.

{% enddocs %}

{% docs column__has_updated_preferences %}

True if the preferences before and after the update is different.

{% enddocs %}

{% docs column__has_updated_subscribed_products %}

True if the subscribed products before and after the update is different.

{% enddocs %}

{% docs column__has_updated_status %}

True if the status of the billing agreement before and after the update is different.

{% enddocs %}

{% docs column__has_updated_loyalty_level %}

True if the loyalty level before and after the update is different.

{% enddocs %}

{% docs column__has_updated_onesub_flag %}

True if the OneSub flag for the billing agreement before and after the update is different.

{% enddocs %}

# Fact Loyalty Orders
{% docs column__pk_fact_loyalty_orders %}

The unique key of each row in Fact Loyalty Orders.

{% enddocs %}

# Dim Loyalty Order Statuses
{% docs column__pk_dim_loyalty_order_statuses %}

The unique key of each row in Dim Loyalty Order Statuses.

{% enddocs %}

# Fact Recipe Billing Agreement Compliancy

{% docs column__pk_fact_recipe_billing_agreement_compliancy %}

The unique key of each row in Fact Recipe Billing Agreement Compliancy.

{% enddocs %}

{% docs column__compliancy_level %}

The compliancy level between a billing agreement id and a recipe id.
It is calculated by comparing the preference combination of the billing agreement and the recipe.

- Compliancy level is 1 if the billing agreement and the recipe have mismatching allergens.
- Compliancy level is 2 if the billing agreement and the recipe have mismatching concepts or taste preferences.
- Compliancy level is 3 if the billing agreement and the recipe have matching taste preferences and concepts.

{% enddocs %}


# Dim All Preference Combinations

{% docs column__pk_preference_combination_id %}

Unique primary key for the preference combinations dimension.

{% enddocs %}


{% docs column__all_preference_id_list %}

List of all preference ids for the preference combination.

{% enddocs %}


{% docs column__allergen_preference_id_list %}

List of allergen preference ids for the preference combination.

{% enddocs %}


{% docs column__concept_preference_id_list %}

List of concept preference ids for the preference combination.

{% enddocs %}


{% docs column__taste_preferences_excluding_allergens_id_list %}

List of taste preference ids excluding allergens for the preference combination.

{% enddocs %}


{% docs column__taste_preferences_including_allergens_id_list %}

List of taste preference ids including allergens for the preference combination.

{% enddocs %}


{% docs column__preference_name_combinations %}

List of all preference names for the preference combination.

{% enddocs %}


{% docs column__allergen_name_combinations %}

List of all allergen preference names for the preference combination.

{% enddocs %}


{% docs column__concept_name_combinations %}

List of all concept preference names for the preference combination.

{% enddocs %}


{% docs column__taste_name_combinations_excluding_allergens %}

List of all taste preference names excluding allergens for the preference combination.

{% enddocs %}


{% docs column__taste_name_combinations_including_allergens %}

List of all taste preference names including allergens for the preference combination.

{% enddocs %}


{% docs column__number_of_allergen_preferences %}

The number of allergen preferences for the preference combination.

{% enddocs %}


{% docs column__number_of_taste_preferences_excluding_allergens %}

The number of taste preferences excluding allergens for the preference combination.

{% enddocs %}


{% docs column__number_of_taste_preferences_including_allergens %}

The number of taste preferences including allergens for the preference combination.

{% enddocs %}

# Dim Loyalty Events
{% docs column__pk_dim_loyalty_events %}

The unique key of each row in Dim Loyalty Events.

{% enddocs %}

# Fact Loyalty Points
{% docs column__pk_fact_loyalty_points %}

The unique key of each row in Fact Loyalty Points.

{% enddocs %}

# Dim Loyalty Periods
{% docs column__pk_dim_loyalty_seasons %}

The unique key of each row in Dim Loyalty Periods. It is a composite key of company_id and loyalty_season_start_date.

{% enddocs %}


{% docs column__loyalty_season_name %}

The name of the loyalty period (e.g. "2025-P1"). 'P' is short for Period.

{% enddocs %}


{% docs column__loyalty_season_year %}

The year in which the loyalty period starts.

{% enddocs %}


{% docs column__loyalty_season_quarter %}

The quarter of the year in which the loyalty period starts.

{% enddocs %}


{% docs column__loyalty_season_start_date %}

The start date of the loyalty period.

{% enddocs %}


{% docs column__loyalty_season_end_date %}

The end date of the loyalty period.

{% enddocs %}

# Dim Discounts

{% docs column__pk_dim_discounts %}

The primary key of dim_discounts.

{% enddocs %}

# Bridge Preference Combinations Preferences

{% docs column__pk_bridge_preference_combinations_preferences %}

The primary key of the bridge_preference_combinations_preferences.

{% enddocs %}
