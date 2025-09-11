with

partnership_rule_combinations as (

    select * from {{ ref('dim_partnership_rule_combinations') }}

)

, partnerships as (

    select * from {{ ref('dim_partnerships') }}

)

, partnership_rules_combinations_bridge as (

    select 
        md5(
            concat(
                partnerships.pk_dim_partnerships
                , partnership_rule_combinations.pk_dim_partnership_rule_combinations
            )
        ) as pk_bridge_partnership_rule_combinations_partnerships
        , partnerships.pk_dim_partnerships as fk_dim_partnerships
        , partnership_rule_combinations.pk_dim_partnership_rule_combinations as fk_dim_partnership_rule_combinations
        , partnership_rule_combinations.partnership_rule_id_combination_list
        , partnership_rule_combinations.partnership_rule_combination_list
        , partnerships.company_partnership_id
        , partnerships.company_id
        , partnerships.partnership_name
        , partnerships.partnership_rule_description
        , partnerships.partnership_rule_id
    
    from partnership_rule_combinations

    left join partnerships
        on array_contains(
            partnership_rule_combinations.partnership_rule_id_combination_list
            , partnerships.partnership_rule_id
        )
        and partnership_rule_combinations.company_partnership_id = partnerships.company_partnership_id

)

select * from partnership_rules_combinations_bridge
