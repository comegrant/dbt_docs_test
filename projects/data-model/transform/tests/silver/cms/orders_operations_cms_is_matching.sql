with 

orders_operations as (

    select * from {{ref('operations__orders')}}

)

, orders_cms as (

    select * from {{ref('cms__billing_agreement_orders')}}

)

, agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{var("future_proof_date")}}'

)

, mismatches as ( 
  select 
    orders_cms.ops_order_id
    , orders_operations.ops_order_id
    , orders_cms.billing_agreement_id
    , orders_operations.billing_agreement_id
    , agreements.company_id
    , orders_operations.company_id
    , orders_cms.menu_week_monday_date
    , orders_operations.menu_week_monday_date
  from orders_cms
  left join agreements
    on orders_cms.billing_agreement_id = agreements.billing_agreement_id
    and agreements.valid_to = '9999-12-31'
  inner join orders_operations
    using (ops_order_id)
  where 
    orders_cms.billing_agreement_id != orders_operations.billing_agreement_id
    or orders_cms.billing_agreement_order_id != orders_operations.billing_agreement_order_id
    or agreements.company_id != orders_operations.company_id
    or orders_cms.menu_week_monday_date != orders_operations.menu_week_monday_date
  order by orders_cms.menu_week_monday_date
)

-- This test fails if more than 13 mismatches are found
-- 13 known cases from 2018 and 2019, where menu_week_monday_date differs
select count(*) as mismatch_count
from mismatches
having count(*) > 13