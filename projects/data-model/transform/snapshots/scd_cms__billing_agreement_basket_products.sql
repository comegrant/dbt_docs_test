{% snapshot scd_cms__billing_agreement_basket_products %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at_not_null',
      invalidate_hard_deletes=True,
    )
}}

select 
*
,coalesce(updated_at, created_at) as updated_at_not_null
from {{ source('cms', 'cms__billing_agreement_basket_product') }}

{% endsnapshot %} 