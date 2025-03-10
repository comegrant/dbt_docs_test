select
    *
from {{ ref('fact_orders') }}
left join {{ ref('dim_products') }}
    on fk_dim_products = pk_dim_products
where recipe_id is null
and product_type_id = '{{ var("velg&vrak_product_type_id") }}'
and billing_agreement_order_line_id is not null
-- TODO: Remove when history is added
and menu_week_monday_date >=  '{{ var("mealbox_adjustments_cutoff") }}'
