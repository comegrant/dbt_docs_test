select distinct
    financial_year as year,
    financial_quarter as quarter,
    financial_month_number as month,
    financial_week as week,
    extract(yearofweek from monday_date) as iso_year,
    extract(week from monday_date) as iso_week,
    monday_date
from prod.gold.dim_dates
where financial_year >= 2021
and day_of_week = 1
order by monday_date
