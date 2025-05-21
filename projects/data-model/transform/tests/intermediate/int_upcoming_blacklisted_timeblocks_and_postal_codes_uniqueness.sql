select 
    timeblock_id,
    menu_year,
    menu_week,
    company_id,
    postal_code,
    count(*) as row_count
from {{ ref('int_upcoming_blacklisted_timeblocks_and_postal_codes') }}
group by
    timeblock_id,
    menu_year,
    menu_week,
    company_id,
    postal_code
having count(*) > 1

