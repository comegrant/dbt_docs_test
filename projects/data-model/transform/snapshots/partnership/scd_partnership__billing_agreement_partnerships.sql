{% snapshot scd_partnership__billing_agreement_partnerships %}

select * from {{ source('partnership', 'partnership__billing_agreement_partnership') }}

{% endsnapshot %} 