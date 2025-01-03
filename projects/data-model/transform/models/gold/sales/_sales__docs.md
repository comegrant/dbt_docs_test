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

{% docs column__has_delivery %}

1 if the order has delivery. E.g., gift card orders does not have a delivery, most other orders does.

{% enddocs %}

{% docs column__is_added_dish %}

Is 1 if the ordered dish was added by the customer and 0 if the ordered dish was preselected for the customer. For all rows that are not representing a dish the field will be null.

{% enddocs %}

{% docs column__is_removed_dish %}

Is 1 if the preselected dish was removed by the customer and 0 if the preselected dish was ordered. For all rows that are not representing a dish the field will be null.

{% enddocs %}

{% docs column__meal_adjustment_subscription %}

The difference between the ordered number of meals and the number of meals the customer subscribes to.

{% enddocs %}

{% docs column__portion_adjustment_subscription %}

The difference between the ordered number of portions and the number of portions the customer subscribes to.

{% enddocs %}

{% docs column__fk_dim_products_preselected %}

Used to fetch product information about the dishes that has been presented to the customer on the webpage before the order was placed. Is 0 for order lines that are not representing a preselected dish.

{% enddocs %}

{% docs column__fk_dim_recipes_preselected %}

Used to fetch recipe information about the dishes that has been presented to the customer on the webpage before the order was placed. Is 0 for order lines that are not representing a preselected dish.

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

The month of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_quarter %}

The quarter of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_year %}

The year of the customers first delivery.

{% enddocs %}

{% docs column__onesub_flag%}

Describes if the customer has a OneSub product or not in their basket. It will not "OneSub" from the time when the customer got OneSub in their basket for each indivisual customer and "Not OneSub" before.

{% enddocs %}

{% docs column__preselector_flag %}

Describes if a customer has been rolled over to the preselector during Onesub launch. The field is changed to "Preselector" at the time preselector was run first time for each individual customer, and is "Not Preselector" before this for each customer. After the launch people get preselector output for the weeks visble in the webpage if they have taken the preference quiz and not made a deviation themselves. For the weeks not visble in the webpage at launch the preselector will run for all customers.

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

{% docs column__full_commit_sha %}

The full commit sha for the model version that was used to generate the output.

{% enddocs %}

{% docs column__short_commit_sha %}

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
