# Partnerships

{% docs column__partnership_id %}
The unique identifier of the partnership.
{% enddocs %}

{% docs column__partnership_name %}
The name of the partnership. E.g. SAS Partnership.
{% enddocs %}

{% docs column__has_loyalty_program %}
True if there is a loyalty component to the partnership. This means customers earn partnership points in their partner account when ordering a minimum number of Cheffelo mealboxes (e.g. Eurobonus points are earned in the customer's SAS account when ordering four or more mealboxes in the case of the SAS Partnership). False if there is no loyalty component to the partnership and customers do not earn partnership points.
{% enddocs %}

# Partnership Rules

{% docs column__partnership_rule_id %}
The unique identifier of the partnership rule.
{% enddocs %}

{% docs column__partnership_rule_name %}
Name of the partnership rule which defines how a customer earns partnership points. E.g. "SAS - 10,000 initial points"
{% enddocs %}

{% docs column__partnership_rule_description %}
Description of how a customer earns partnership points under the partnership rule. E.g. "SAS - 10.000 one-time initial points after 4th order"
{% enddocs %}

{% docs column__order_criteria %}
The minimum number of orders required for the rule to be applied.
{% enddocs %}

{% docs column__partnership_points_value %}
The number of partnership points awarded when the rule criteria is met.
{% enddocs %}

{% docs column__allow_multiple_uses %}
True if the rule can be applied multiple times. False if the rule can be applied just once. E.g. allow_multiple_uses = true, partnership_rule_criteria = 5, and partnership_rule_value = 250 means a customer will earn 250 partnership points on their 5th order, and each order thereafter.
{% enddocs %}

{% docs column__is_cumulative %}
True if the rule applies with other rules concurrently. If a customer places their nth order which matches multiple rules, and all rules are cumulative, all rules are applied. If there's a mix of cumulative and non-cumulative rules, the non-cumulative rule with the lowest criteria is applied.
{% enddocs %}

# Company Partnerships

{% docs column__company_partnership_id %}
The unique identifier of the brand-partnership relationship.
{% enddocs %}

{% docs column__is_active_company_partnership %}
True if the company_partnership relationship is active. False otherwise.
{% enddocs %}

# Company Partnership Rules

{% docs column__company_partnership_rule_id %}
The unique identifier of the company-specific partnership rule.
{% enddocs %}

{% docs column__is_active_company_partnership_rule %}
True if the company partnership relationship is active.
{% enddocs %}

# Billing Agreement Partnerships

{% docs column__billing_agreement_partnership_id %}
The unique identifier of the billing agreement partnership relationship.
{% enddocs %}

# Billing Agreement Partnership Loyalty Points

{% docs column__billing_agreement_partnership_loyalty_point_id %}
The unique identifier of the partnership loyalty points transaction.
{% enddocs %}

{% docs column__billing_agreement_partnership_loyalty_points_parent_id %}
The identifier of the parent partnership loyalty points transaction, if this is a child transaction.
{% enddocs %}

{% docs column__billing_agreement_partnership_loyalty_points_event_id %}
The identifier of the event that triggered this partnership loyalty points transaction.
{% enddocs %}

{% docs column__transaction_points_partnership %}
The number of partnership loyalty points earned on the transaction.
{% enddocs %}