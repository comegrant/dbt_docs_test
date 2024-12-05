with predict_set as (
    select distinct
        recipe_id
    from
        mlgold.weekly_dishes_variations
    where (menu_year * 100 + menu_week) >= {predict_start_yyyyww}
        and  (menu_year * 100 + menu_week) <= {predict_end_yyyyww}
        and company_id = '{company_id}'
)

select * from predict_set
