
select 
  menu_year, 
  menu_week, 
  product_variation_id, 
  count(distinct menu_id) 
from {{ ref('fact_menus') }}
group by menu_year, menu_week, product_variation_id
having count(distinct menu_id) > 1
order by menu_year, menu_week