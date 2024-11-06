{% snapshot scd_cms__billing_agreement_consents %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='billing_agreement_consent_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

with source as (
  select * from {{ source('cms', 'cms__billing_agreement_consent') }}
)

select 
  concat(agreement_id, consent_id) as billing_agreement_consent_id
  , source.* 
from source

{% endsnapshot %} 
