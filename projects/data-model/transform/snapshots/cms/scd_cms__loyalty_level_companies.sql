{% snapshot scd_cms__loyalty_level_companies %}

-- config disabled until cms backend changes implemented
select * from {{ source('cms', 'cms__loyalty_level_company') }}

{% endsnapshot %} 