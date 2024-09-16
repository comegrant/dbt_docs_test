{% snapshot scd_cms__billing_agreement_basket_products %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='created_at',
    )
}}

select * from {{ source('cms', 'cms__billing_agreement_basket_product') }}

{% endsnapshot %} 