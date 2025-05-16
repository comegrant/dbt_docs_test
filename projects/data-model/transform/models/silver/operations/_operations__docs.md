# Cases

{% docs column__case_id %}

The primary key of the case table. Contains a unique id for each case.

{% enddocs %}

{% docs column__case_redelivery_timeblock_id %}

The new timeblock assigned to an order that could not be delivered in the original timeblock and has been assigned a redelivery. 

{% enddocs %}

{% docs column__case_status_id %}

The status of the case. Foreign key to the case status table. Can have status id 0-4.

{% enddocs %}

{% docs column__case_redelivery_id %}

The status of the redelivery. Foreign key to the case status table. Can have status id 0-4.

{% enddocs %}

{% docs column__case_redelivery_comment %}

Comment connected to redelivery. Can come from external partners or internally from logistics/customer service, which is defined by source_updated_by.

{% enddocs %}

# Case Lines

{% docs column__case_line_id %}

The primary key of the case lines table.

{% enddocs %}

{% docs column__case_line_cause_id %}

The foreign key to the case line causes table.

{% enddocs %}

{% docs column__case_line_responsible_id %}

The foreign key to the case line responsible table.

{% enddocs %}

{% docs column__case_line_category_id %}

The foreign key to the case line categories table.

{% enddocs %}

{% docs column__case_line_comment %}

Comments regarding the case. Can come from external partners or internally from logistics/customer service, which is defined by source_updated_by.

{% enddocs %}

{% docs column__case_line_amount %}

The amount the customer will get in reimbursement. I.e., the cost of the case.

{% enddocs %}

{% docs column__is_active_case_line %}

Describes whether or not the case line is active. Is active unless a line is cancelled or deleted. 1 is active and 0 is inactive.

{% enddocs %}

# Case Line Types

{% docs column__case_line_type_id %}

The primary key for the case line types table. 

{% enddocs %}

{% docs column__case_line_type_name %}

Contains the description of the different case line types.

{% enddocs %}

# Case Line Ingredients

{% docs column__supplier_name %}

The name of the ingredient supplier.

{% enddocs %}

{% docs column__ingredient_price %}

The price of the ingredient in a case line. Price is set per ingredient by customer service. From 2025 there can be more than one ingredient per case line. Ingredient price multiplied by ingredient quantity is equal to the amount paid to customer for a case.

{% enddocs %}

{% docs column__ingredient_quantity %}

The quantity of ingredients registered in the case. 

{% enddocs %}


# Case Causes

{% docs column__case_cause_id %}

The primary key for the case cause table.

{% enddocs %}

{% docs column__case_cause_name %}

Contains the cause name related to the case. From week 2 2025 cause name refers to the priority of the case.

{% enddocs %}

# Case Categories

{% docs column__case_category_id %}

The primary key for the case category table.

{% enddocs %}

{% docs column__case_category_name %}

Contains the name of the category or department which can be related to the cases.

{% enddocs %}

# Case Responsible

{% docs column__case_responsible_id %}

The primary key of the case responsible table.

{% enddocs %}

{% docs column__case_responsible_description %}

Contains what reason or which company/supplier was responsible for a case happening. From 2025 this only contains the reason/explanation for a case, not company/supplier.

{% enddocs %}