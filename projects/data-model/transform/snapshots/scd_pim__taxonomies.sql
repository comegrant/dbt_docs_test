{% snapshot scd_pim__taxonomies %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='taxonomies_id',

      strategy='timestamp',
      updated_at='modified_date',
    )
}}

select * from {{ source('pim', 'pim__taxonomies') }}

{% endsnapshot %} 