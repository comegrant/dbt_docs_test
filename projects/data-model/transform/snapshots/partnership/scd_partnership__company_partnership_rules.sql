{% snapshot scd_partnership__company_partnerships %}

select * from {{ source('partnership', 'partnership__company_partnership') }}

{% endsnapshot %} 