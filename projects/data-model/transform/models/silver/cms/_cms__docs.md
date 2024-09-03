# Ids

{% docs column__company_id %}

The primary key in the database. 

{% enddocs %}

{% docs column__country_id %}

The primary key of the country in the cms database. 

{% enddocs %}

{% docs column__agreement_id %}

The id of an billing agreement.

{% enddocs %}

{% docs column__cms_order_id %}

This is the primary key in the CMS database for each placed order.

{% enddocs %}

{% docs column__ops_order_id %}

This is the primary key in the OPS database for each placed order. Is null if there was no order generated in OPS. This could for instance be if a customer only buy a gift card or an order was cancelled. 

{% enddocs %}

{% docs column__order_line_id %}

This is the primary key of the order line in CMS.

{% enddocs %}

{% docs column__order_type_id %}

This is the primary key of the order type in CMS.

{% enddocs %}

{% docs column__order_status_id %}

This is the primary key of the order status in CMS.

{% enddocs %}

# Strings

{% docs column__company_name %}

The name of the company.

{% enddocs %}

{% docs column__country_name %}

The name of the country.

{% enddocs %}

{% docs column__billing_agreement_status_name %}

The name of the status of the billing agreement:
* `active`: agreement is active meaning they will take a delivery in the week following cut-off.
* `freezed`: agreement has frozen meaning they will not take a delivery in the week following cut-off.
* `pending`: agreement has been created (customer has signed up) but is yet to take a delivery. 
* `bad player`: (avida) Customer has a poor payment history.
* `postal area closed`: Postal area where the customer is registered is closed.
* `fraud`: Fraudulent customer.
* `locked`: Customer's account is locked.
* `deleted`: Customer's account is deleted.
* `leads`: Legacy, used previously in LMK?

{% enddocs %}

{% docs column__order_type_name %}

The name of the order type:
* `Order After Registration`: Orders of new customers that registered after cut-off. Currently only available for Godtlevert. Have not been in use since 2022.
* `Daily Direct Order`: Orders with next day delivery. This belongs to Godtlevert Flex a discontinued service that found place in 2019 and 2020.
* `Loyalty`: Has never been used.
* `Recurring`: Regular weekly subscription. I.e., orders that follows cut-off and our fixed delivery slots.
* `Campaign`: Special mealboxes such as easter and christmas mealboxes. Order is direct and does not follow the regular cut-off and delivery slots. 
* `Gift Card`: Gift Card purchases. Can be done without being a registered customer. Order is direct and does not follow the regular cut-off and delivery slots.
* `Orders after cutoff`: Orders that are createdby customer service after cut-off. Have not been in use since 2022.

Total number of orders in one week would hence be: `Orders after registration + Orders after cut-off + Recurring`

{% enddocs %}

{% docs column__order_status_name %}

The status of the order:
* `Created`: Order is generated in CMS. Happens at cut-off, order later maybe? Can be cancelled.
* `Processing`: Order is moved to logistics. Payment reduction (PRP) can be added and order can be cancelled.
* `Hold`: ??. Order can be cancelled.
* `Waiting`: ??
* `Cancelled`: Order is cancelled. More about this here. Examples on Why, when, what. 
* `Finished`: Order is locked. When does this happen? XX days after delivery?. If being credited that will happen together with the next order gen.

{% enddocs %}

{% docs column__order_line_type_name %}

The type of the order line.

* `Credit`: Credited amounts that is due to a case for a previous order. 
* `Debit`: Purchased products
* `Debit_Gift`: Purchased marketing gifts
* `Discount`: Discounts on the order
* `Discount_Gift`: Not sure how this differ from discount.
* `Invoice`: Invoice fee.
* `Loyalty_Credit`: Credits which can be purchased in the loyalty points store
* `P4P`: Plate for plate (Swedish charity program)
* `PRP`: Credited amounts that is related to a case for the current order.
* `Transport`: Transport fee (a.k.a Delivery Fee, Freight Free)

{% enddocs %}

{% docs column__user_type %}

userType only exists for 40 records from 2021 for one agreement id that belongs to Retnemt. The purpose of this column is unknown.

{% enddocs %}

# Numerics

{% docs column__variation_qty %}

Quntity ordered of the variation.

{% enddocs %}

{% docs column__order_line_unit_price_ex_vat %}

The unit price of the ordered variation for the specific order ex vat.

{% enddocs %}

{% docs column__order_line_unit_price_inc_vat %}

The unit price of the ordered variation for the specific order inc vat. Calculated from order line table by multiplying the unit price ex vat with (1 + vat).

{% enddocs %}

{% docs column__order_line_total_amount_ex_vat %}

The total amount of the order line ex vat. Calculated by multiplying the price ex vat with the quantity.

{% enddocs %}

{% docs column__order_line_total_amount_inc_vat %}

The total amount of the order line inc vat. Calculated by multiplying the price ex vat with the quantity and (1+vat).

{% enddocs %}

{% docs column__order_line_vat_percent %}

The vat of the order line in percent.

{% enddocs %}

{% docs column__order_line_vat %}

The vat of the order.

{% enddocs %}

# Booleans
{% docs column__order_has_recipe_leaflets %}

Flag to indiacte if order should have printed recipes.

Business context: From February 2024 customers got the option to opt out of printed recipes. 

{% enddocs %}

{% docs column__order_status_can_be_cancelled %}

Is `1` if the order is in a state where it can be cancelled.

{% enddocs %}

{% docs column__order_type_is_direct_order %}

Orders that are not generated in relation to cut-off. 

{% enddocs %}

{% docs column__order_type_allow_anonymous %}

Is one if the order can be placed without beeing a registrered customer. Such as for gift cards.

{% enddocs %}

{% docs column__order_type_is_direct_payment %}

Is one if the order is paid for directly while purchasing. Such as for gift cards.

{% enddocs %}

# Time

{% docs column__delivery_year %}

The delivery year of the placed order

{% enddocs %}

{% docs column__delivery_week %}

The delivery week of the placed order

{% enddocs %}

{% docs column__delivery_week_monday_date %}

The first date in the delivery week (starts on a Monday).

{% enddocs %}

{% docs column__first_delivery_week_monday_date %}

The [Delivery Week Date](#Delivery-Week-Date) of the first all time delivery of the customer. 

{% enddocs %}

# Source System Fields
{% docs column__order_created_at %}

The date the order was created. Not meaning when the order was initially placed, but when it was generated by CMS. This is usually at cutoff for regular orders, but for gift card or special campaigns such as easter lamb and so on it does not necessarily follow cut off. 

{% enddocs %}