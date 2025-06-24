{% snapshot scd_pim__price_categories %}

select 
    md5(concat(level, portion_id, company_id)) as ck_price_category
    , * 
from {{ source('pim', 'pim__price_category') }}

{% endsnapshot %}
