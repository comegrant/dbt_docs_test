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

{% docs column__distribution_center_type_name %}

The name of the distribution center type. Can be HUB, DIP or Packing.

{% enddocs %}

# Distribution Centers
{% docs column__distribution_center_id %}

The unique id of the distribution center in the source database.

{% enddocs %}

{% docs column__distribution_center_name %}

The name of the distribution center namne. A distribution center is the site where the delivery gets distributed to the transport company. An order can go through several distibution centers during the delivery process.

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

{% docs column__case_line_total_amount_inc_vat %}

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

{% docs column__case_impact_id %}

In January 2025 the `case_cause_id`-column was changed to representing the impact rather than the cause. 
Hence, an additional column has been created representing only ids after this change was fully launched in the system (28.01.2025).

{% enddocs %}

{% docs column__case_cause_responsible_id %}

In January 2025 the `case_responsible_id`-column was changed to representing the cause and the `case_cause_id`-column was changed to representing the impact on the customer experience.
Due to this an additional id-column has been created generating a new id based on the `case_cause_id` for all cases until 28.01.2025, and based on `case_responsible_id` after this. 
To ensure uniqueness, the id has been concatenated with 'C' for cause and 'R' for responsible respectively, then hashed.

{% enddocs %}

# Case Line Ingredients

{% docs column__ingredient_price %}

The price of the ingredient in a case line. Price is set per ingredient by customer service. From 2025 there can be more than one ingredient per case line. Ingredient price multiplied by ingredient quantity is equal to the amount paid to customer for a case.

{% enddocs %}

{% docs column__ingredient_quantity_case %}

The quantity of ingredients registered in the case. 

{% enddocs %}


# Case Causes

{% docs column__case_cause_id %}

The unique id for the case cause table.

{% enddocs %}

{% docs column__case_cause_name %}

Contains the cause name related to the case. From end of January 2025 this column was changed to contain the impact of the case on the customer experience (Low, Medium, High, Goodwill). In the beginning the old and new column values was used in parallell but from Februray 2025 the transition was fully made. This has been taken into account in the `operations__case_lines`-table which in turn is used in `Dim Case Details` to clean the data.

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

Originially contained what reason or which company/supplier was responsible for a case happening. From end of January 2025 this contains the cause of the case. 
This has been taken into account in the `operations__case_lines`-table which in turn is used in `Dim Case Details` to clean the data.

{% enddocs %}

{% docs column__case_responsibility_type %}

Contains whether the responsible party for a case is internal or an external partner.

{% enddocs %}

# Case Taxonomies

{% docs column__taxonomy_id_operations %}

The id's belonging to the different case taxonomies.
{% enddocs %}

# Taxonomies

{% docs column__taxonomy_name_operations %}

The name of the taxonomy which is related to a case. Can for instance be missing product, partial delivery or late cancellation.

{% enddocs %}

{% docs column__is_active_taxonomy %}

Column containing whether or not the taxonomy is active in frontend (in use/not in use).

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
