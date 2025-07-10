# Dim Transportation

{% docs column__last_mile_distribution_site %}

The last distribution site of the order before being delivered to the customer.

{% enddocs %}

{% docs column__production_site %}

The distribution center where the order was packed before delivery.

{% enddocs %}

# Dim Case Details
{% docs column__pk_dim_case_details %}

Primary key of the dim_case_details table.

{% enddocs %}

# Fact Cases
{% docs column__pk_fact_cases %}

Primary key of the fact_case table.

{% enddocs %}

{% docs column__case_line_total_amount_inc_vat %}

The total amount the customer will be reimbursed for a case line including vat. 

> ⚠️ Note: This column should not be used for analytics purposes, as it can lead to duplication when a single case line contains multiple ingredients. For accurate analytics, use `case_line_amount` instead.
{% enddocs %}

{% docs column__case_line_amount_ex_vat %}

The amount the customer will be reimbursed excluding vat, allocated proportionally across each ingredient in the case line.

When a case line contains multiple ingredients, the total amount is distributed based on each ingredient's share of the total price.

**Example**:  
If a case line includes:
- 2 bananas at 15 kr each, and  
- 1 carrot at 5 kr,  

Then the total value is: `2 * 15 + 5 = 35 kr`  
- Bananas share: `30 / 35 = 0.86`  
- Carrot share: `5 / 35 = 0.14`

Each ingredient gets a corresponding share of the total reimbursement amount.

{% enddocs %}

{% docs column__case_line_amount_inc_vat %}

The amount the customer will be reimbursed including vat, allocated proportionally across each ingredient in the case line.

When a case line contains multiple ingredients, the total amount is distributed based on each ingredient's share of the total price.

**Example**:  
If a case line includes:
- 2 bananas at 15 kr each, and  
- 1 carrot at 5 kr,  

Then the total value is: `2 * 15 + 5 = 35 kr`  
- Bananas share: `30 / 35 = 0.86`  
- Carrot share: `5 / 35 = 0.14`

Each ingredient gets a corresponding share of the total reimbursement amount.

{% enddocs %}

{% docs column__is_complaint %}

True if the case line belongs to a complaint and false for other case lines such as sms, notes or redeliveries. Complaints are delivery incidents reported by the customers, customer service or our external partners (i.e. transport company) which can result in compensating the customer. The complaint can relate to the delivery itself or to the products being delivered.

{% enddocs %}

{% docs column__is_accepted_redelivery %}

True if the case line belongs to a redelivery that has been approved and rescheduled by the logistics team. The flag is on case line level. The information about the redelivery (i.e., new timeblock etc) is on case level. There can only be one accepted redelivery case line per case.

{% enddocs %}
