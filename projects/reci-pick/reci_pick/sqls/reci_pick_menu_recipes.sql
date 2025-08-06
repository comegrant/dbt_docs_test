select
    *
from mlgold.reci_pick_menu_recipes
where
    (menu_year * 100 + menu_week) >= {start_yyyyww}
    and company_id = '{company_id}'
