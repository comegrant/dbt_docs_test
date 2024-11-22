{% snapshot scd_pim__recipe_taxonomies_list %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='recipe_id',
      strategy='check',
      check_cols=['taxonomy_ids_list'],
    )
}}

with 

source as (

  select * from {{ source('pim', 'pim__recipes_taxonomies') }}

)

, concatenate_taxonomies_ids_to_list as (
  select 
    recipe_id
  , array_join(sort_array(array_agg(taxonomies_id)),',') as taxonomy_ids_list
  from source
  group by recipe_id
  order by recipe_id
)

select * from concatenate_taxonomies_ids_to_list

{% endsnapshot %} 