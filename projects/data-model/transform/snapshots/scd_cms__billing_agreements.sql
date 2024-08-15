{% snapshot scd_cms__billing_agreements %}

{{
    config(
      target_database=target.database,
      target_schema='snapshots_temp',
      unique_key='agreement_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('cms', 'cms__billing_agreement') }}

{% endsnapshot %} 