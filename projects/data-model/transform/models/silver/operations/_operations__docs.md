# Transport Companies

{% docs column__transport_company_id %}

The unique id of the transport company in the source database.

{% enddocs %}

{% docs column__transport_company_name %}

The name of the transport company

{% enddocs %}

# Distribution Center Types
{% docs column__distribution_center_type_id %}

The unique id of the distribution center type in the source database.

{% enddocs %}

{% docs column__distribution_center_type__name %}

The name of the distribution center type. [Q: What is this actually?]

{% enddocs %}

# Distribution Centers
{% docs column__distribution_center_id %}

The unique id of the distribution center in the source database.

{% enddocs %}

{% docs column__distribution_center_name %}

The name of the distribution center namne. A distribution center is .. [Q: What is this actually?]

{% enddocs %}

{% docs column__distribution_center_postal_code %}

The postal code of the distribution center

{% enddocs %}

{% docs column__distribution_center_latitude %}

The latitude of the placement of the distribution center.

{% enddocs %}

{% docs column__distribution_center_longitude %}

The longtiude of the placement of the distribution center.

{% enddocs %}

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

{% docs column__redelivery_status_id %}

The status of the redelivery. Can have status id 0-4. There is currently no related table with the name or descriprtion of each status. Status = 2 means accepted and is currently the only status that is relevant for reporting.

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

True if the case line is active, else false. If the case line is inactive it means that it has been deleted on the Operations UI.

{% enddocs %}

# Case Line Types

{% docs column__case_line_type_id %}

The unique id for the case line types table. 

{% enddocs %}

{% docs column__case_line_type_name %}

Contains the name of the different case line types. An order can only have one case, but many case lines. A case line can be related to what type of compensation the customer receives, if it's a redelivery or if it's an sms or note relating to the order. Customers can get three different types of compensation: 1. Refund: Transfer to the customers card/bank account, 2. Credit: The credited amount gets added to the customers credit balance giving them a deduction on future orders, 3. PRP: A deduction on the order belonging to the case of the customer (before 2025 PRP could also be put on future active orders not belonging to the current case), to get an PRP the order can not be locked for invoicing.

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

# Cutoff Calendar
{% docs column__cutoff_calendar_id %}

The unique identifier for a row in the cutoff calendar.

{% enddocs %}

{% docs column__cutoff_at_local_time %}

The local timestamp when cutoff takes place for the corresponding menu week.

{% enddocs %}

{% docs column__cutoff_at_utc %}

The UTC timestamp when cutoff takes place for the corresponding menu week.

{% enddocs %}

# Cutoff
{% docs column__cutoff_id %}

The unique identifier for a row in the cutoffs table.

{% enddocs %}

{% docs column__cutoff_name %}

The name of the cutoff. There is a cutoff for each country and an additional cutoff for Godt Matlyst (external company). Cutoff is a timestamp when orders become locked for a particular menu week and country. It is usually scheduled for the same time every week. Changes are often enforced around red days. 

{% enddocs %}

{% docs column__cutoff_description %}

Describes when in the week cutoff usually occurs.  

{% enddocs %}

# Geofences

{% docs column__geofence_id %}

Unique identifier for the geofence.

{% enddocs %}

{% docs column__geofence_layer_id %}

ID of the geofence layer.

{% enddocs %}

{% docs column__geofence_container_id %}

ID of the geofence container.

{% enddocs %}

{% docs column__geofence_polygon_id %}

ID of the geofence polygon.

{% enddocs %}

{% docs column__geofence_layer_name %}

Name of the geofence layer, an object created in Atlas - our visualization tool for delivery areas, which may contain one or more containers.

{% enddocs %}

{% docs column__geofence_container_name %}

Name of the geofence container, an object created in Atlas - our visualization tool for delivery areas, which may contain one or more polygons.

{% enddocs %}

{% docs column__geofence_polygon_name %}

Name of the geofence polygon, a geographical area defined by a set of coordinates in Atlas - our visualization tool for delivery areas. 

{% enddocs %}

{% docs column__is_active_geofence %}

True if the geofence is active for operations. False if it is not active.

{% enddocs %}

# Geofence Postal Codes

{% docs column__geofence_postal_code_id %}

Unique identifier of a row in the operations__geofence_postalcode table.

{% enddocs %}

{% docs column__is_active_geofence_postal_code %}

True if the postal code is actively connected to the associated geofence. False otherwise.

{% enddocs %}
