select 
    * 
from {{ ref('fact_orders') }}
where recipe_id is null
and is_dish = true
and billing_agreement_order_line_id != 0
-- TODO: Remove when history is added
and menu_week_monday_date >= '{{ var("onesub_beta_launch_date") }}'