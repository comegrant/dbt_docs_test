{% snapshot scd_cms__billing_agreements_preferences_list %}

{{
    config(
      target_database=target.database,
      target_schema='dbt_snapshots',
      unique_key='agreement_id',

      strategy='check',
      check_cols = ['preference_id_list', 'billing_agreement_preference_id_list'],
      updated_at='updated_at_modified',
    )
}}

with 

source as (

  select * from {{ source('cms', 'cms__billing_agreement_preference') }}

)

, ranked as (
  select 
  id
  , agreement_id
  , preference_id
  , updated_at
  , updated_by
  , row_number() over (partition by agreement_id order by updated_at desc) as rank
  from source
)

, latest_updates as (
  select 
  agreement_id
  , updated_at
  , updated_by
  from ranked
  where rank = 1
)

, preference_list as (
  select 
  agreement_id
  , array_sort(collect_list(preference_id)) as preference_id_list
  , array_sort(collect_list(id)) as billing_agreement_preference_id_list
  from source
  group by agreement_id
)

, list_and_updates_joined as (
  select 
  preference_list.agreement_id
  , preference_id_list
  , billing_agreement_preference_id_list
  , latest_updates.updated_at
  , latest_updates.updated_by
  from preference_list
  left join latest_updates
  on preference_list.agreement_id = latest_updates.agreement_id
)

, updated_at_column_modified as (
  --This is because if there has been deletes then the updated_at field might not be updated and we should use the getdate field instead.
  select 
  agreement_id
  , preference_id_list
  , billing_agreement_preference_id_list
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