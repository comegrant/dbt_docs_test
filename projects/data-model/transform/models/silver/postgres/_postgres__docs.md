# Preferences Updated
{% docs column__preference_updated_id %}

Unique id generated in silver table for preferences_updated. Concatination of
* `ce_preference_updated_id`
* `company_id`

{% enddocs %}

{% docs column__ce_preference_updated_id %}

Id from the postgres tables called `ce_preferences_updated` in each schema for each brand.

{% enddocs %}

{% docs column__concept_preference_id %}

The concept preference that belonged to the customer at the time preferences were updated.

{% enddocs %}

{% docs column__concept_preference_list %}

List of concept preferences updated by a customer. Pre OneSub this is usually one concept per customer. After OneSub this could be multiple concepts per customer.

{% enddocs %}

{% docs column__taste_preference_list %}

List of taste preferences (negative prefrerences) updated by a customer.

{% enddocs %}
