-- Makes sure there is one and only one timestamp per company with is_latest_estimation = true

select
    company_id
    , count(distinct estimation_generated_at) as distinct_estimation_generated_at
from {{ ref('fact_estimations') }}
where is_latest_estimation = true
group by 1
having
    -- check if there is no estimation with is_latest_estimation = true
    sum(case when is_latest_estimation = true then 1 else 0 end) = 0
    -- check if there is more than one estimation with is_latest_estimation = true
    or count(distinct estimation_generated_at) <> 1
