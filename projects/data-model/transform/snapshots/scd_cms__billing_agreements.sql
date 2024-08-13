{% snapshot scd_cms__billing_agreements %}

{{
    config(
      target_database='dev',
      target_schema='snapshots_temp',
      unique_key='agreement_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('cms', 'cms_billing_agreement') }}

{% endsnapshot %} 