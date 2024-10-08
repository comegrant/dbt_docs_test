{% snapshot scd_cms__billing_agreement_preferences %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('cms', 'cms__billing_agreement_preference') }}

{% endsnapshot %} 