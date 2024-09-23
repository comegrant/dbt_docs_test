# Common for Segment models
{% docs column__source_received_at %}

Timestamp of when the event was received by Segment

{% enddocs %}

{% docs column__source_sent_at %}

Timestamp of when the event was sent to Segment

{% enddocs %}


# Deviations Ordered
{% docs column__deviations_ordered_id %}

Unique id in the table. This is a hash of the following columns: 
- the original id in the source table, renamed to ce_deviation_ordered_id in this silver table
- brand (linas/adams/gl/retnemt)
- product_variation_id

{% enddocs %}

{% docs column__ce_deviation_ordered_id %}

The original id from the source tables

{% enddocs %}

{% docs column__action_done_by %}

The id or name of the person or system that created the deviation.

{% enddocs %}

{% docs column__event_text %}

The event text. So far it is allways ceDeviationOrdered.

{% enddocs %}

{% docs column__product_variation_quantity_deviations_ordered %}

Quantity of the deviating variation. 
Note: _All_ variations in the basket will become a deviation if at least _one_ variation in the basket is changed.

{% enddocs %}

{% docs column__taste_preferences %}

List of taste preferences (preferences of type "Taste") for the customer.

{% enddocs %}

{% docs column__concept_preferences %}

List of concept preferences (preferences of type "Concept") for the customer.

{% enddocs %}