# Fact Orders
{% docs column__pk_fact_orders %}

The unique key of each row in Fact Orders. Is a composite key of:
- billing_agreement_order_id
- billing_agreement_order_line_id
- product_variation_id
- preselected_product_variation_id
- recipe_id
- preselected_recipe_id

{% enddocs %}

{% docs column__weeks_since_first_order %}

Number of weeks since the first order of the billing agreement.

{% enddocs %}

{% docs column__preselected_product_variation_id %}

The id of the product variations the customer would have in their basket if not making any changes to their order.

{% enddocs %}

{% docs column__preselected_recipe_id %}

The recipe id of the dish the customer would have in their basket if not making any changes to their order. I.e. the dish presented to the customer of the frontend.

{% enddocs %}

{% docs column__meals_mealbox %}

The number of meals in the mealbox the customer ordered. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Meals using the meals related to the mealbox of the order. With the connection its possible to create analysis such answering questions such as: 
- How does the composition of recipes differ among customers that order different number of meals?


{% enddocs %}

{% docs column__meals_mealbox_subscription %}

The number of meals in the mealbox the customer subscribes to. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Meals using the meals related to the mealbox in the customers subscription. The connection makes it possible to create analysis answering questions such as:
- How does the composition of recipes differ among customers that subscribed to different number of meals?

{% enddocs %}

{% docs column__portions_mealbox %}

The number of portions in the mealbox the customer ordered. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Portions using the portions related to the mealbox of the order. The column are mainly present for debugging purposes.

{% enddocs %}

{% docs column__portions_mealbox_subscription %}

The number of portions in the mealbox the customer subscribes to. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Portions using the portions related to the mealbox in the customers subscription. The column are mainly present for debugging purposes.

{% enddocs %}

{% docs column__portion_id_mealbox %}

The id of the number of portions in the mealbox the ordered. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Portions using the portions related to the mealbox of the order. The connection makes it possible to create analysis answering questions such as:
- How does the composition of recipes differ among customers that subscribed to different number of portions?

{% enddocs %}

{% docs column__portion_id_mealbox_subscription %}

The the id of the number of portions in the mealbox the customer subscribes to. The column is on order level, meaning that the value is present on all the rows of the order. It is used to create a foreign key connecting all rows of the order to Dim Portions using the portions related to the mealbox in the customers subscription. The connection makes it possible to create analysis answering questions such as:
- How does the composition of recipes differ among customers that subscribed to different number of portions?

{% enddocs %}

{% docs column__mealbox_servings %}

The total number of servings in the mealbox which is calculated by taking meals * portions * product quantity. The value is null for all product variations that are not represention a mealbox.

{% enddocs %}

{% docs column__mealbox_servings_subscription %}

The total number of servings in the subscribed mealbox which is calculated by taking meals * portions * subscribed product quantity. The value is null for all product variations that are not represention a mealbox.

{% enddocs %}

{% docs column__dish_servings %}

The number of servings on the dish, which is calculated by taking dish quantity * portions. The value is null for rows that is not a dish in the order (is_dish = true).

{% enddocs %}

{% docs column__dish_servings_subscription %}

The number of servings in the preselected dish, which is calculated by taking subscribed quantity * portions. The value is null for rows that is not a preselected dish.

{% enddocs %}

{% docs column__has_normal_order_type %}

True if the order type is one of the following:
- Recurring
- Order After Registration
- Orders After Cutoff
- Daily Direct Order
- Campaign

The flag is on order level and will be used in Power BI to only include the orders of interest.

{% enddocs %}

{% docs column__has_subscription_order_type %}

True if the order type is one of the following:
- Recurring
- Order After Registration
- Orders After Cutoff

The flag is on order level and will be used in Power BI to only include the orders of interest.

{% enddocs %}

{% docs column__has_finished_order_status %}

True if the order status is one of the following:
- Finished
- Processing

The flag is on order level and will be used in Power BI to only include the orders of interest.

{% enddocs %}

{% docs column__has_plus_price_dish %}

1 for all the rows in an order if the customer has chosen one or more premium dishes in their mealbox, else 0. When a premium dish is selected an extra cost will be added to the order. Is null if the order does not have any dishes.

{% enddocs %}

{% docs column__has_thrifty_dish %}

1 for all the rows in an order if the customer has chosen one or more thrifty dishes in their mealbox, else 0. When a thrifty dish is selected a price reduction is added to the order. Is null if the order does not have any dishes.

{% enddocs %}

{% docs column__has_swap %}

True if the customer replaced the dishes in the menu presented to them on the frontend with other dishes, else false.

The order is not considered to have a swap if any of these are true, else the field will be false.
- The customer did not make any changes to the dishes on their order, and sticked with what was presented to them on the webpage.
- The customer only removed/added dishes without adding/removing another dish
- The customer did not get any preselected menu on the frontend, and was forced to chose dishes themselves (see is_missing_preselector-column)

The flag is on order level. I.e. all the rows for the order will have the same value for this flag.

The field is null for non subscription orders and orders created before we started tracking subscribed products (<2024).

{% enddocs %}

{% docs column__has_mealbox_adjustment %}

True if the customer has made any adjustments to their mealbox either by swapping dishes, changing number of meals or changing number of portions, else false.

The flag is on order level. I.e. all the rows for the order will have the same value for this flag. 

The field is null for non subscription orders and orders created before we started tracking subscribed products (<2024).

{% enddocs %}

{% docs column__is_mealbox %}

Is true if the product variation represents a mealbox else false. If there are several mealboxes on the order it will be true of all mealboxes.

{% enddocs %}

{% docs column__is_subscribed_mealbox %}

Is true if the preselected product variation represents the subscribed mealbox else false.

{% enddocs %}

{% docs column__is_preselected_dish %}

Is true if the preselected product variation represents a dish, else false.

{% enddocs %}

{% docs column__is_added_dish %}

Is 1 if the ordered dish was added by the customer themselves and 0 if the ordered dish was a part of the preselection the customer was presented to on the frontend. For all rows that are not representing a dish the field is null. Is used to determine which type of dishes customers add to their menu.

{% enddocs %}

{% docs column__is_removed_dish %}

Is 1 if the customer removed the dish presented on the frontend from their menu and and 0 if the dish presented on the frontend was ordered by the customer. For all rows that are not representing a dish the field is null. Is used to determine which type of dishes customers remove from their menu.

{% enddocs %}

{% docs column__is_thrifty_dish %}

Customers can select dishes that give them a price decrease. Column is 1 if the customer was discounted for the dish on their order, 0 for all dishes that did not have a minus price and null if the product variation is not a dish.

{% enddocs %}

{% docs column__is_plus_price_dish %}

Customers can select dishes for an extra price. The column is 1 if the customer was charged extra for the dish on their order, 0 for all dishes that did not have an extra price and null if the product variation is not a dish.

{% enddocs %}

{% docs column__is_grocery %}

True if the the product variation represents a grocery, else false.

{% enddocs %}

{% docs column__is_subscribed_grocery %}

True if the preselected product variation represents a grocery the customer subscribes to, else false. Is null for all orders that was placed before we started tracking subscribed products (< 2024).

{% enddocs %}

{% docs column__is_onesub_migration %}

Flag that indicates if the order is a part of the migration to Onesub and is on order level. I.e. the whole order will have the same value for this flag. Is 1 if the order was a part of the Onesub migration else 0. The flag is used to determine if the customer should have recieve preselector output yet or not. Its also used for other debugging purposes.

{% enddocs %}

{% docs column__is_missing_preselector_output %}

Flag that indicates if the customer was presented for output of the preselector in the frontend before selecting recipes. Due to bugs it can happen that a customer does not see any preselected dishes in their menu on the frontend. When this happens the customer will often just go ahead and select dishes themselves. If the customer was not presented with preselector output in the frontend the value of the flag is 1, if the customer did receive preselector output before selecting dishes its 0. The value is null for orders that existed before the preselector was launched together with Onesub (pre fall of 2024). The flag is on order level. I.e. the whole order will have the same value for this flag.

{% enddocs %}

{% docs column__meal_adjustment_subscription %}

The difference between the number of meals ordered by the customer and the number of meals they subscribe to. The differnece is on the mealbox row of the order.

{% enddocs %}

{% docs column__portion_adjustment_subscription %}

The difference between the number of portions ordered by the customer and the number of portions they subscribe to. The customer can change portions on dish level, hence is the number placed on the recipe row of the order.

{% enddocs %}

{% docs column__product_variation_quantity_subscription %}

Quantity subscribed to of the product.

{% enddocs %}

{% docs column__dish_quantity %}

Number of dishes ordered.

{% enddocs %}

{% docs column__dish_quantity_subscription %}

Number of dishes in subscription. I.e. the quantity of the dish in the customers basket before making any changes.

{% enddocs %}

{% docs column__total_amount_ex_vat_subscription %}

The total amount of the order line if only including subscribed product variation quantity. E.g. if a customer subscribed to one milk and adds two more. This column will only contain the amount coming from one milk that was subscribed to.

{% enddocs %}

{% docs column__price_category_cost %}

The planned cost of ingredients (with quantity in whole units) for a single unit of the recipe, represented by `total_ingredient_planned_cost_whole_units`. This value is not multiplied by `dish_quantity` and reflects the cost for one recipe only. It is used to determine the price category in `dim_price_categories`.

{% enddocs %}

{% docs column__fk_dim_products_subscription %}

Used to fetch product information about the dishes that has been presented to the customer on the webpage before the order was placed. Is 0 for order lines that are not representing a preselected dish.

{% enddocs %}

{% docs column__fk_dim_recipes_subscription %}

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

{% docs column__sum_added_dish %}

The sum of the is_added_dish column for each order. Used for middle calculations and debugging.

{% enddocs %}

{% docs column__sum_removed_dish %}

The sum of the is_removed_dish column for each order. Used for middle calculations and debugging.

{% enddocs %}

{% docs column__orders_subscriptions_match_key %}

A key used to match ordered products with preselected/subscribed products. Is the recipe id if a recipe exists, else its the product variation id.

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

{% docs column__reactivation_date %}

The date of the monday of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__reactivation_week %}

The week of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__reactivation_month %}

The month name of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__reactivation_month_number %}

The month number of the of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__reactivation_quarter %}

The quarter of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__reactivation_year %}

The year of of the delivery which makes a customer enter the reactivated customer journey segment.

{% enddocs %}

{% docs column__has_grocery_subscription %}

True if the customer has groceries in their subscription, else false.

{% enddocs %}

{% docs column__onesub_flag%}

Describes if the customer has a OneSub product or not in their basket. It will not "OneSub" from the time when the customer got OneSub in their basket for each individual customer and "Not OneSub" before.

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

The primary key of Dim Order Types. Is a composite key of the order type id coming from the source table and the mealbox_composition_id which is generated in the dimension.

{% enddocs %}

{% docs column__mealbox_composition_id %}

An id representing the mealbox composition. The id is generated inside the dimension together with all the column values it represents.

{% enddocs %}

{% docs column__mealbox_selection %}

A description of how the meals in the mealbox was selected. Can be preselected menu or customer composed. Customer composed meaning that the customer has made adjustments to the dishes in the mealbox and preselected menu meaning that the customer has chosen the default selection.

{% enddocs %}

{% docs column__premium_dish %}

Describes if an order has a premium dish or not.

{% enddocs %}

{% docs column__thrifty_dish %}

Describes if an order has a thrifty dish or not.

{% enddocs %}

# Dim Order Line Details

{% docs column__pk_dim_order_line_details %}

The unique id of the rows in Dim Order Line Details

{% enddocs %}

{% docs column__order_line_details %}

Details on the type of order line. Such as plus price dish, thrifty dish, groceries, mealbox etc.

{% enddocs %}

# Dim Preferences

{% docs column__pk_dim_preferences %}

Primary key of the preferences dimension. It is a composite key of preference_id and company_id.

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

{% docs column__has_upgraded_loyalty_level %}

True if the loyalty level number after the update is higher than before the update.

{% enddocs %}

{% docs column__has_downgraded_loyalty_level %}

True if the loyalty level number after the update is lower than before the update.

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

# Dim Preference Combinations

{% docs column__pk_dim_preference_combinations %}

Primary key of the preference combination dimension.

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

{% docs column__number_of_preferences %}

The number of preferences of all types in the preference combination.

{% enddocs %}

{% docs column__number_of_concept_preferences %}

The number of concept preferences in the preference combination.

{% enddocs %}

# Dim Loyalty Events
{% docs column__pk_dim_loyalty_events %}

The unique key of each row in Dim Loyalty Events.

{% enddocs %}

# Fact Loyalty Points
{% docs column__pk_fact_loyalty_points %}

The unique key of each row in Fact Loyalty Points.

{% enddocs %}

{% docs column__level_booster_points %}

The number of points that are earned on this transaction due to a multiplier. For example, if a customer earns 150 points with a 1.5x multiplier, the transaction points component earned from this multiplier is 50 points.

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

# Dim Addresses
{% docs column__pk_dim_addresses %}

The unique key of each row in Dim Addresses.

{% enddocs %}

# Dim Discounts

{% docs column__pk_dim_discounts %}

The primary key of dim_discounts.

{% enddocs %}

{% docs column__is_discount_chain %}

Defines if the discount is part of a discount chain or not.

{% enddocs %}

# Bridge Preference Combinations Preferences

{% docs column__pk_bridge_preference_combinations_preferences %}

The primary key of the bridge_preference_combinations_preferences.

{% enddocs %}

# Dim Customer Journey Segments

{% docs column__pk_dim_customer_journey_segments %}

The primary key of the dim_customer_journey_segments. It is a hash of the column customer_journey_sub_segment_id.

{% enddocs %}


# Dim Partnerships

{% docs column__pk_dim_partnerships %}

The primary key of dim_partnerships. It is a hash of the columns company_partnership_id and partnership_rule_id.

{% enddocs %}

# Fact Partnership Points

{% docs column__pk_fact_partnership_points %}

The primary key of fact_partnership_points.

{% enddocs %}

{% docs column__fk_dim_dates_partnership_points_generated_at %}

Foreign key to dim dates on the date when partnership points are generated.

{% enddocs %}


# Fact Billing Agreements Daily


{% docs column__pk_fact_billing_agreements_daily %}

The primary key of fact_billing_agreements_daily. It is a hash of the columns date and billing_agreement_id.

{% enddocs %}

{% docs column__is_monday %}

True if the date represents a monday, else False. 

{% enddocs %}

{% docs column__is_paused %}

Indicates if the customer has paused their delivery on the week represented by the monday. True if paused, false if not. Null for non-Mondays.

{% enddocs %}

{% docs column__is_active %}

True if the agreement status is "Active", else false.

{% enddocs %}

{% docs column__is_freezed %}

True if the agreement status is "Freezed", else false.

{% enddocs %}

{% docs column__has_order %}

Indicates if the customer has taken a delivery on the week represented by the monday. True if the customer has taken an order, false if not. Null for non-Mondays.

{% enddocs %}


# Dim Partnership Rule Combinations

{% docs column__pk_dim_partnership_rule_combinations %}

The primary key of dim_partnership_rule_combinations.

{% enddocs %}

{% docs column__partnership_rule_combinations_id %}

md5 hash of the partnership_rule_id_combination_list column or '0' if partnership_rule_id_combination_list = ['0'].

{% enddocs %}

{% docs column__partnership_rule_id_combination_list %}

Ordered array of partnership_rule_id combinations

{% enddocs %}

{% docs column__partnership_rule_combination_list %}

Ordered array of partnership_rule_description combinations

{% enddocs %}


# Bridge Partnerships Partnership Rules

{% docs column__pk_bridge_partnership_rule_combinations_partnerships %}

The primary key of bridge_partnership_rule_combinations_partnerships. It is a hash of the columns fk_dim_partnerships and fk_dim_partnership_rule_combinations.

{% enddocs %}
