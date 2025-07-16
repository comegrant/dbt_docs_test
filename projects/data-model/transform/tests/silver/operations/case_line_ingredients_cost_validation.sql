with

cases as (

    select * from {{ref('operations__cases')}}

)

, case_lines as (

    select * from {{ref('operations__case_lines')}}

)

, case_line_ingredients as (

    select * from {{ref('operations__case_line_ingredients')}}

)

, orders as (

    select * from {{ref('operations__orders')}}

)

, case_line_cost_mismatch as (

    select 
        menu_year
        , menu_week
        , case_line_id
        , case_line_total_amount_inc_vat
        , sum(ingredient_price * ingredient_quantity) as ingredient_amount
        , abs(case_line_total_amount_inc_vat - sum(ingredient_price * ingredient_quantity)) as abs_diff
    from case_line_ingredients
    left join case_lines using(case_line_id)
    left join cases using(case_id)
    left join orders using(ops_order_id)
    
    where is_active_case_line is true
    and case_line_type_id != 6
    and menu_year = 2025
    and menu_week >= 6
    
    group by all
    having case_line_total_amount_inc_vat != sum(ingredient_price * ingredient_quantity)

)

select 
    menu_week
    , sum(abs_diff) as total_abs_diff
from case_line_cost_mismatch
group by 1
having sum(abs_diff) > 200