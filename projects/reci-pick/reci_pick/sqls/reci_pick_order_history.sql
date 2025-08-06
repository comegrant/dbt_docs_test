select
    *
from mlgold.reci_pick_order_history
where
    company_id = '{company_id}'
    and (menu_year * 100 + menu_week) >= {start_yyyyww}
