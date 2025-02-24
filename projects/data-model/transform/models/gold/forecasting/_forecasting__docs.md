# Fact Estimations
{% docs column__pk_fact_estimations %}
Primary key of the fact_estimations table
{% enddocs %}

{% docs column__fk_dim_date_estimation_generated %}
Foreign key connecting dim_date to the day at which an estimation was generated at.
{% enddocs %}

{% docs column__fk_dim_time_estimation_generated %}
Foreign key connecting dim_time to the time at which an estimation was generated at.
{% enddocs %}

{% docs column__fk_dim_date_menu_week %}
Foreign key connecting the menu week to dim_date.
{% enddocs %}

{% docs column__estimations_product_variation_quantity %}
The quantity of the associated product variation that would have been ordered if cutoff were to have occured at the time at which the estimation was generated.
{% enddocs %}

{% docs column__is_latest_estimation %}
A boolean flag indicating whether this estimation is from the most recent generation.
{% enddocs %}


# Dim Budget

{% docs column__pk_dim_budget_types %}
Primary key of the dim_budget_types table
{% enddocs %}

# Fact Budget

{% docs column__pk_fact_budget %}
Primary key of the fact_budget table
{% enddocs %}

{% docs column__fk_dim_date %}
Foreign key connecting the budget dates to dim_date
{% enddocs %}

{% docs column__fk_dim_companies %}
Foreign key connecting the companies table to the budget table
{% enddocs %}

{% docs column__fk_dim_budget_types %}
Foreign key connecting the budget table to the budget_type table
{% enddocs %}