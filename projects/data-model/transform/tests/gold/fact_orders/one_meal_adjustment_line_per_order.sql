select 
    billing_agreement_order_id
    , count(*)
from {{ ref('fact_orders') }}
where meal_adjustment_subscription is not null
group by billing_agreement_order_id 
having count(*) > 1