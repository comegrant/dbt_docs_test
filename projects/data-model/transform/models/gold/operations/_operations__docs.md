# Dim Timeblocks

{% docs column__pk_dim_timeblocks %}

Primary key of the dim_timeblocks table.

{% enddocs %}

{% docs column__delivery_window_name %}

The delivery window represented on the format "Mon 14:00-16:00".

{% enddocs %}

{% docs column__delivery_time_window_name %}

The hours in the delivery window represented on the format "14:00-16:00".

{% enddocs %}

# Dim Transportation

{% docs column__pk_dim_transportation %}

Primary key of the dim_transportation table.

{% enddocs %}

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

{% docs column__case_impact %}

The impact on the customer experience due to the case. Originates from case_cause_name.

{% enddocs %}

{% docs column__case_cause_name_dim %}

Contains the cause name related to the case. Originates from case_cause_name before February 2025 and from case_responsible_description after due to changes in the source system.

{% enddocs %}

# Fact Cases
{% docs column__pk_fact_cases %}

Primary key of the fact_case table.

{% enddocs %}

{% docs column__case_line_total_amount_inc_vat_fact %}

The total amount the customer will be reimbursed for a case line including vat. 

> ⚠️ Note: This column should not be used for analytics purposes, as it can lead to duplication when a single case line contains multiple ingredients. For accurate analytics, use `credit_amount` instead.
{% enddocs %}

{% docs column__credit_amount_ex_vat %}

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

{% docs column__credit_amount_inc_vat %}

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

{% docs column__case_line_ingredient_amount_inc_vat %}

The amount the ingredient costs based on the unit price and quantity reported by customer service when the case was created. 

From menu week 6 2025 and onwards it's possible to have *one* case line with *several* ingredients and this amount should be the same as `credit_amount_inc_vat` unless the `case_line_type_id = 6` (no credit).

Before menu week 6 2025 this data cannot be trusted due to mistakes made when reporting:
1. One case line was created for each ingredient
2. One case line was created with one selected ingredient, and the additional ingredients were manually typed into the comment field.
3. Credits not related to the ingredients was placed on the same case line as the ingredient cost

{% enddocs %}

{% docs column__case_line_ingredient_amount_share %}

The ingredient's share of the total ingredient cost for the case line. Before menu week 6 2025 this field cannot be trusted. After this it should be correct (but bugs can occur). Tests have been created to validate this field.

{% enddocs %}

{% docs column__is_complaint %}

True if the case line belongs to a complaint and false for other case lines such as sms, notes or redeliveries. Complaints are delivery incidents reported by the customers, customer service or our external partners (i.e. transport company) which can result in compensating the customer. The complaint can relate to the delivery itself or to the products being delivered.

{% enddocs %}

{% docs column__is_accepted_redelivery %}

True if the case line belongs to a redelivery that has been approved and rescheduled by the logistics team. The flag is on case line level. The information about the redelivery (i.e., new timeblock etc) is on case level. There can only be one accepted redelivery case line per case.

{% enddocs %}
