# Timeblocks
{% docs column__timeblock_id %}

The ID of the timeblock. A timeblock describes a delivery window that consists of a day of the week and a time range during that day when deliveries can be made.

{% enddocs %}

# Timeblock Blacklist
{% docs column__timeblocks_blacklisted_id %}

The unique identifier for a blacklisted timeblock.

{% enddocs %}

{% docs column__fallback_timeblock_id %}

The ID of the alternative timeblock to use when the primary timeblock is blacklisted.

{% enddocs %}

# Postal Codes
{% docs column__postal_code_id %}

ID of a postal code as it appears in operations database.

{% enddocs %}

{% docs column__postal_code %}

Postal code as it would be read or written by a human.

{% enddocs %}

{% docs column__city_name %}

The name of the city associated with this postal code.

{% enddocs %}

{% docs column__county_name %}

The name of the county associated with this postal code.

{% enddocs %}

{% docs column__municipality_name %}

The name of the municipality associated with this postal code.

{% enddocs %}

{% docs column__is_active_postal_code %}

True if this postal code is currently active, false otherwise.

{% enddocs %}

{% docs column__has_geofence %}

True if this postal code has an associated geofence, false otherwise.

{% enddocs %}

# Zones
{% docs column__zone_id %}

The unique identifier for a delivery zone.

{% enddocs %}

{% docs column__is_active_zone %}

True if this zone is currently active, false otherwise.

{% enddocs %}

{% docs column__transport_company_id %}

The unique identifier for a transport company that operates in this zone.

{% enddocs %}

{% docs column__menu_year_week_from_zones %}

The first menu week that this zone is valid for, formatted as YYYYWW (ISO).

{% enddocs %}

{% docs column__menu_year_week_to_zones %}

The last menu week that this zone is valid for, formatted as YYYYWW (ISO).

{% enddocs %}

# Cases

{% docs column__case_id %}

The unique id of the case table. Contains a unique id for each case.

{% enddocs %}

{% docs column__redelivery_timeblock_id %}

The new timeblock assigned to an order that could not be delivered in the original timeblock and has been assigned a redelivery. 

{% enddocs %}

{% docs column__case_status_id %}

The status of the case. Foreign key to the case status table. Can have status id 0-4.

{% enddocs %}

{% docs column__redelivery_id %}

The status of the redelivery. Foreign key to the case status table. Can have status id 0-4.

{% enddocs %}

{% docs column__redelivery_comment %}

Comment connected to redelivery. Can come from external partners or internally from logistics/customer service, which is defined by source_updated_by.

{% enddocs %}

{% docs column__redelivery_at %}

Timestamp of the redelivery.

{% enddocs %}

# Case Lines

{% docs column__case_line_id %}

The unique id of the case lines table.

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

The unique id for the case line types table. 

{% enddocs %}

{% docs column__case_line_type_name %}

Contains the description of the different case line types.

{% enddocs %}

# Case Line Ingredients

{% docs column__ingredient_price %}

The price of the ingredient in a case line. Price is set per ingredient by customer service. From 2025 there can be more than one ingredient per case line. Ingredient price multiplied by ingredient quantity is equal to the amount paid to customer for a case.

{% enddocs %}

{% docs column__ingredient_quantity %}

The quantity of ingredients registered in the case. 

{% enddocs %}


# Case Causes

{% docs column__case_cause_id %}

The unique id for the case cause table.

{% enddocs %}

{% docs column__case_cause_name %}

Contains the cause name related to the case. From week 2 2025 cause name refers to the priority of the case.

{% enddocs %}

# Case Categories

{% docs column__case_category_id %}

The unique id for the case category table.

{% enddocs %}

{% docs column__case_category_name %}

Contains the name of the category or department which can be related to the cases.

{% enddocs %}

# Case Responsible

{% docs column__case_responsible_id %}

The unique id of the case responsible table.

{% enddocs %}

{% docs column__case_responsible_description %}

Contains what reason or which company/supplier was responsible for a case happening. From 2025 this only contains the reason/explanation for a case, not company/supplier.

{% enddocs %}
