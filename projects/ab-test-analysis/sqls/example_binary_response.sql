select distinct
    has_order,
    case
        when rand() < 0.5 then 'a'
        else 'b'
    end as random_group,
    menu_year,
    menu_week,
    feedback,
    billing_agreement_id,
    fk_dim_companies
from gold.fact_preselected_recipe_feedback
where
    (menu_year * 100 + menu_week) >= 202440
