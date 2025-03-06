# Preselector output

{% docs column__taste_preference_compliancy_code %}

The degree to which the preselector was able to produce a set of meals that complied with the taste preferences selected by the customer

- 3: all taste preferences are upheld
- 2: allergy taste preferences are upheld, but we have broken some non-allergy taste preferences for at least one meal e.g. pork when they selected pork as a taste preference
- 1: we have broken allergy taste preferences for at least one meal

{% enddocs %}

{% docs column__model_version_commit_sha %}

The full commit SHA of the model version used to create the output

{% enddocs %}

{% docs column__preselector_error_vector %}

The list of error values for each feature that the preselector takes into consideration.

A measure of how much the preselector output deviates from the target value for this metric. The error is calculated as the absolute difference between the target and actual value, normalized to a 0-100 scale where 0 means no error (perfect match) and 100 means maximum possible error.

{% enddocs %}

{% docs column__is_override_deviation %}

Whether or not the customer's own deviations were overwritten by the preselector deviations

{% enddocs %}

{% docs column__has_data_processing_consent %}

Whether or not the customer has consented to data processing

{% enddocs %}

{% docs column__target_cost_of_food_per_meal %}

The target cost of food per meal that the preselector used to create the output

{% enddocs %}

{% docs column__requested_meals %}

The number of meals that the preselector was asked to output

{% enddocs %}

{% docs column__requested_portions %}

The number of portions that the preselector was asked to output

{% enddocs %}

{% docs column__preselector_error_message %}

The error message that the preselector returned

{% enddocs %}

{% docs column__preselector_error_code %}

The error code that the preselector returned. Can be one of the following:

- 1 = Found no menu for year, week and company id
- 2 = Unable to find cost of food for menu week
- 3 = Also related to cost food for a menu week, but for a slightly different dataset
- 500 = Unknown error, that we currently do not handle. Leads to an early exit in the batch predict pipeline

{% enddocs %}

{% docs column__correlation_id %}

The unique ID of the request to track across microservices

{% enddocs %}

{% docs column__main_recipe_quarantining_control %}

A dictionary argument that is used for controlling quarantining (removing them from consideration) of main recipes. You can pass in the main_recipe_id and the yearweek of the last time it was ordered by the customer, if this yearweek is within the bounds of the quarantine period, the recipe will not be considered.

{% enddocs %}

{% docs column__requested_menu_year_weeks %}

The weeks and years that the preselector was asked to compute for

{% enddocs %}

{% docs column__created_at_preselector_output %}

The timestamp from when the preselected output that set of meals.

{% enddocs %}

{% docs column__menu_week_output_version %}

The preselector can output and overwrite each menu week's pre-selection multiple times, either because the customer has changed their preferences or because the preselector batch job from CMS has been run again. This column indicates which sequential version of the output it is for that menu week per billing agreement.

{% enddocs %}

{% docs column__is_most_recent_output %}

The preselector can output and overwrite each menu week's pre-selection multiple times, either because the customer has changed their preferences or because the preselector batch job from CMS has been run again. This column indicates whether the output is the latest version or not for that menu week per billing agreement. Note that this may not be the same as the latest set of meals that the customers sees on the frontend, because they may have made a deviation themselves.

{% enddocs %}
