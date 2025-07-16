select 
  case_line_id
  , avg(case_line_total_amount_inc_vat)
  , sum(credit_amount_inc_vat)
from from {{ ref('fact_cases') }}
group by 1
having round(avg(case_line_total_amount_inc_vat),2) - sum(credit_amount_inc_vat) != 0