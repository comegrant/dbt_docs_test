# Forecast Jobs

{% docs column__forecast_job_id %}

Unique identifier for a forecast job. A forecast job comprises of one or more forecast models being applied to one or more forecast groups to produce output in forecasting.

{% enddocs %}

{% docs column__forecast_job_name %}

The name of the forecast job.

{% enddocs %}

# Forecast Job Run Metadata

{% docs column__forecast_job_run_metadata_id %}

The unique identifier for a row in forecast job run metadata.

{% enddocs %}

{% docs column__forecast_job_run_id %}

The ID of the forecast job run the metadata pertains to.

{% enddocs %}

{% docs column__forecast_job_run_parent_id %}

The ID of the parent job run. If job run B is triggered by job run A, then A is the parent to B. This column is the empty string '' if the row does not have a parent.

{% enddocs %}

{% docs column__next_cutoff_menu_week %}

The menu week of the upcoming cutoff.

{% enddocs %}

{% docs column__forecast_horizon %}

The number of weeks in the horizon of the Total Order Forecast job run (related by the forecast_job_run_parent_id) when a forecast is created. The horizon is either 1, 11 or 15.

{% enddocs %}

{% docs column__menu_week_monday_date_next_cutoff %}

The Monday date to the menu week of the upcoming cutoff.

{% enddocs %}

{% docs column__forecast_horizon_index %}

The position of a menu week within the forecast horizon, relative to the menu week of the upcoming cutoff. The next cutoff menu week has forecast_horizon_index = 1, and subsequent weeks increment by 1.

{% enddocs %}

{% docs column__is_most_recent_menu_week_horizon_forecast %}

Boolean flag indicating whether this is the most recent forecast for the combination of menu week and horizon index. A menu week will appear at multiple positions in the forecast horizon as time progresses, for example when the menu week is 4 cutoffs away, 3 cutoffs away, 2 cutoffs away, etc. It is possible that a forecast can be run multiple times for a menu week at a certain position in the forecast horizon. This column will be true for the latest forecast of each menu week at each position in the horizon.

{% enddocs %}

{% docs column__is_most_recent_menu_week_forecast %}

Boolean flag indicating whether this is the most recent forecast for a given menu week, regardless of horizon index.

{% enddocs %}

# Forecast Groups

{% docs column__forecast_group_id %}

The unique identifier of a forecast group.

{% enddocs %}

{% docs column__forecast_group_name %}

A forecast group is a forecasting construct representing a subset of customers who are expected to perform a certain action, for example, make adjustments to their mealboxes. The forecast group is also a component which is used to determine which forecast model will be applied.

{% enddocs %}

# Forecast Models

{% docs column__forecast_model_id %}

The unique identifier of a forecast model.

{% enddocs %}

{% docs column__forecast_model_name %}

A forecast model is an algorithm which is applied to create a forecast for a specific forecast group, product type and menu week in the future.

{% enddocs %}

# Forecast Orders

{% docs column__forecast_orders_id %}

The unique identifier for a row in forecast orders.

{% enddocs %}

{% docs column__order_quantity_forecast %}

The forecasted number of orders for a given forecast group and menu week.

{% enddocs %}

# Forecast Variations

{% docs column__forecast_variations_id %}

The unique identifier for a row in forecast variations.

{% enddocs %}

{% docs column__product_variation_quantity_forecast_analytics %}

The forecasted quantity of a given product variation for a given forecast group and menu week.

{% enddocs %}
