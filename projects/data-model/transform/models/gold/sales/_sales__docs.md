# Fact Orders
{% docs column__pk_fact_orders %}

The unique key of each row in Fact Orders.

{% enddocs %}

{% docs column__weeks_since_first_order %}

Number of weeks since the first order of the billing agreement.

{% enddocs %}

{% docs column__preselected_product_variation_id %}

The product variation of the preselected mealbox that corresponds to the customer composed mealbox (pre Onesub).

{% enddocs %}

{% docs column__preselected_recipe_id %}

The recipe is of the preselected mealbox that corresponds to the customer composed mealbox (pre Onesub).

{% enddocs %}

{% docs column__has_delivery %}

The order has a delivery related to it.

{% enddocs %}

{% docs column__is_added_dish %}

The recipe has been selected by the customer.

{% enddocs %}

{% docs column__is_removed_dish %}

The preselected recipe has been removed by the customer. 

{% enddocs %}

{% docs column__is_generated_recipe_line %}

The row has been generated to add the recipe.

{% enddocs %}

{% docs column__is_chef_composed_mealbox %}

The order line referes to a mealbox and its recipes that are preselected by a chef (pre Onesub).

{% enddocs %}

{% docs column__is_mealbox %}

The order line is a part of an mealbox order.

{% enddocs %}

{% docs column__fk_dim_products_preselected %}

Foreign key that is used to relate preselected product variations to Dim Products.

{% enddocs %}

{% docs column__fk_dim_recipes_preselected %}

Foreign key that is used to relate preselected recipes to Dim Recipes.

{% enddocs %}

# Dim Billing Agreements

{% docs column__pk_dim_billing_agreements %}

The unique id of each row in Dim Billing Agreements

{% enddocs %}

{% docs column__first_menu_week_monday_date %}

The weeks monday date of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_week %}

The week of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_month %}

The month of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_quarter %}

The quarter of the customers first delivery.

{% enddocs %}

{% docs column__first_menu_week_year %}

The year of the customers first delivery.

{% enddocs %}

{% docs column__is_onesub%}

Agreements with Onesub Mealbox

{% enddocs %}

# Dim Order Statuses

{% docs column__pk_dim_order_statuses %}

The unique id of the rows in Dim Order Statuses

{% enddocs %}

# Dim Order Types

{% docs column__pk_dim_order_types %}

The unique id of the rows in Dim Order Types

{% enddocs %}

# Dim Preferences

{% docs column__pk_dim_preferences %}

Primary key of the preferences dimension. It is a composite key of preference_id and company_id. 

{% enddocs %}

# Bridge Billing Agreements Preferences

{% docs column__pk_bridge_billing_agreements_preferences %}

...

{% enddocs %}

{% docs column__billing_agreement_preferences_updated_id %}

...

{% enddocs %}