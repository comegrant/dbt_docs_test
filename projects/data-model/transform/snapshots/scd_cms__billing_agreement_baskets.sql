{% snapshot scd_cms__billing_agreement_baskets %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('cms', 'cms__billing_agreement_basket') }}

{% endsnapshot %} 