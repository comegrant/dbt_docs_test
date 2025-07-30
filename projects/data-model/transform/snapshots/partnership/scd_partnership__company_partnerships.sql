{% snapshot scd_partnership__company_partnership_rules %}

select * from {{ source('partnership', 'partnership__company_partnership_rule') }}

{% endsnapshot %} 