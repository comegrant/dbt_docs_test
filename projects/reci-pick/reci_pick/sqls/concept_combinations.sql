with concept_preferences as (
    select
        pk_dim_preferences,
        preference_id,
        company_id,
        preference_type_id,
        preference_name,
        preference_type_description
    from
        {env}.gold.dim_preferences
    where preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- concept preferences
    and company_id = '{company_id}'
),

bridge as (
    select
        fk_dim_preferences,
        fk_dim_preference_combinations
    from {env}.gold.bridge_preference_combinations_preferences
),

preference_combinations as (
    select
        pk_dim_preference_combinations,
        concept_preference_id_list,
        concept_name_combinations
    from {env}.gold.dim_preference_combinations
)

select
    preference_combinations.*
from preference_combinations
inner join bridge
    on preference_combinations.pk_dim_preference_combinations = bridge.fk_dim_preference_combinations
inner join concept_preferences
    on bridge.fk_dim_preferences = concept_preferences.pk_dim_preferences
