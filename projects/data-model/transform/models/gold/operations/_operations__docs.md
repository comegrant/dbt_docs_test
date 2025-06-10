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
