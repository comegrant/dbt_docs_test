with 

preferences as (
    select * from {{ ref('cms__preferences') }}
)

, preference_types as (

    select * from {{ ref('cms__preference_types') }}

)

, preferences_companies as (

    select * from {{ ref('cms__preferences_companies') }}

)

, tables_joined as (

    select 
      preferences.preference_id
    , coalesce(preferences_companies.company_id,'0') as company_id
    , preferences.preference_type_id
    , preferences.preference_name_general
    , preferences.preference_description_general
    , coalesce(preferences_companies.preference_name, 'Unknown') as preference_name
    , coalesce(preferences_companies.preference_description, 'Unknown') as preference_description
    , preference_types.preference_type_name
    , preference_types.preference_type_description
    , coalesce(preferences_companies.is_active_preference, false) as is_active_preference
    , preferences.is_allergen
    from 
    preferences
    left join preference_types
    on preferences.preference_type_id = preference_types.preference_type_id
    left join preferences_companies
    on preferences.preference_id = preferences_companies.preference_id

)

, add_pk as (

    select 

    md5(concat(
        cast(preference_id as string),
        cast(company_id as string)
        )
    ) AS pk_dim_preferences
    , tables_joined.*
    
    from 
    tables_joined

)

select * from add_pk