{% snapshot scd_cms__billing_agreement_basket_products_list %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='billing_agreement_basket_id',
      strategy='check',
      check_cols = ['products_list'],
      updated_at='updated_at_modified',
    )
}}

with 

source as (

  select * from {{ source('cms', 'cms__billing_agreement_basket_product') }}

)

, ranked as (
  select 
  id
  , billing_agreement_basket_id
  , subscribed_product_variation_id
  , delivery_week_type
  , quantity
  , is_extra
  , coalesce(updated_at, created_at) as updated_at
  , updated_by
  , created_at
  , row_number() over (partition by billing_agreement_basket_id order by coalesce(updated_at, created_at) desc) as rank
  from source
)

, latest_updates as (
  select 
  billing_agreement_basket_id
  , updated_at
  , updated_by
  from ranked
  where rank = 1
)

, basket_products_list as (
  select 
  billing_agreement_basket_id
  , array_sort(
      collect_list(
        struct(
          subscribed_product_variation_id as product_variation_id 
          , quantity as product_variation_quantity
          , is_extra)
      )
   ) as products_list
  from source
  group by billing_agreement_basket_id
)

, list_and_updates_joined as (
  select 
  basket_products_list.billing_agreement_basket_id
  , basket_products_list.products_list
  , latest_updates.updated_at
  , latest_updates.updated_by
  from basket_products_list
  left join latest_updates
  on basket_products_list.billing_agreement_basket_id = latest_updates.billing_agreement_basket_id
)

, updated_at_column_modified as (
  --This is because if there has been deletes then the updated_at field might not be updated and we should use the getdate field instead.
  select 
  billing_agreement_basket_id
  , products_list
  , updated_at
  , updated_by
  , case
      when 
      {% if table_exists(this) %} 
        updated_at <= (select max(updated_at_modified) from {{this}}) 
      {% else %}
        false
      {% endif %}
      then from_utc_timestamp(current_timestamp(), 'Europe/Stockholm')
      else updated_at
    end as updated_at_modified
  from list_and_updates_joined
)

select * from updated_at_column_modified

{% endsnapshot %} 