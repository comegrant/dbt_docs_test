with loyalty_order_line_points as (
    select
        loyalty_order_id,
        sum(unit_point_price * product_variation_quantity) as total_line_points 
    from {{ ref('cms__loyalty_order_lines') }}
    group by loyalty_order_id
),

loyalty_order_points as (
    select
        id as loyalty_order_id,
        total_points
    from {{ source('cms','cms__loyalty_order') }}
)

select 
    loyalty_order_points.loyalty_order_id,
    loyalty_order_points.total_points as order_total_points,
    loyalty_order_line_points.total_line_points as sum_of_line_points
from loyalty_order_points
left join loyalty_order_line_points
    on loyalty_order_points.loyalty_order_id = loyalty_order_line_points.loyalty_order_id
where loyalty_order_points.total_points != loyalty_order_line_points.total_line_points
