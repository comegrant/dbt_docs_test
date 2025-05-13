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

basket_products as (

  select * from {{ source('cms', 'cms__billing_agreement_basket_product') }}

)

, baskets as (

  select * from {{ source('cms', 'cms__billing_agreement_basket') }}

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
  from basket_products
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
  baskets.id as billing_agreement_basket_id
  , array_sort(
      collect_list(
        struct(
          basket_products.subscribed_product_variation_id as product_variation_id 
          , basket_products.quantity as product_variation_quantity
          , basket_products.is_extra
          )
      )
   ) as products_list
  from baskets
  left join basket_products
    on baskets.id = basket_products.billing_agreement_basket_id
  group by all
)

, list_and_updates_joined as (
  select 
  basket_products_list.billing_agreement_basket_id
  , basket_products_list.products_list
  , coalesce(latest_updates.updated_at, baskets.updated_at) as updated_at
  , coalesce(latest_updates.updated_by, baskets.updated_by) as updated_by
  from basket_products_list
  left join latest_updates
  on basket_products_list.billing_agreement_basket_id = latest_updates.billing_agreement_basket_id
  left join baskets
  on basket_products_list.billing_agreement_basket_id = baskets.id
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