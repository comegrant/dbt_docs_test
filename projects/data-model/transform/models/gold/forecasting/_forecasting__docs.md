# Fact Estimations
{% docs column__pk_fact_estimations %}

Primary key of the fact_estimations table

{% enddocs %}

{% docs column__estimation_generated_at %}

Timestamp for when the estimation was generated (the estimation is generated within the Data Platform).

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

# Fact Forecast Variations

{% docs column__pk_fact_forecast_variations %}

Primary key of the fact_forecast_variations table. It is an md5-hashed string of the columns: menu_year, menu_week, company_id, forecast_group_id, forecast_model_id, product_variation_id, forecast_job_run_id, forecast_generated_at.

{% enddocs %}

{% docs column__product_variation_quantity_forecast %}

The forecasted quantity of the associated product variation, including any manual overwrites that have been applied.

{% enddocs %}

{% docs column__fk_dim_dates_menu_week %}

Foreign key connecting the menu week of the forecast to dim_dates.

{% enddocs %}

{% docs column__fk_dim_dates_forecast_generated_at %}

Foreign key connecting the forecast generation date to dim_dates.

{% enddocs %}


# Dim Forecast Runs

{% docs column__pk_dim_forecast_runs %}

Primary key of the dim_forecast_runs table.

{% enddocs %}

{% docs column__forecast_horizon_group %}

The number of weeks in the horizon of the Total Order Forecast when a forecast is created, grouped as either either '1' or '11/15'.

{% enddocs %}
