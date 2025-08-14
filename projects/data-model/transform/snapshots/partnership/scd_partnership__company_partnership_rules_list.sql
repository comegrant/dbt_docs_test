{% snapshot scd_partnership__company_partnership_rules_list %}

select
    company_partnership_id
    , array_sort(collect_list(partnership_rule_id)) as partnership_rule_ids_list
from {{ source('partnership', 'partnership__company_partnership_rule') }}
where is_active = true
group by company_partnership_id

{% endsnapshot %} 