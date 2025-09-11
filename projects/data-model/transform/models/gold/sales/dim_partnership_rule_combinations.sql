with 

partnership_order_rule_combinations as (

    select * from {{ ref('int_billing_agreement_partnership_order_rule_combinations') }}

)

, partnerships_rules as (

    select * from {{ ref('int_company_partnerships_rules_joined') }}

)

-- get distinct combinations of rules that exist on all orders where rules have been applied
, partnership_rule_combinations_distinct as (

    select distinct
        partnership_rule_combinations_id
        , company_partnership_id
        , company_id
        , partnership_id
        , partnership_rule_id_combination_list
        , partnership_rule_combination_list
    from partnership_order_rule_combinations

)

-- get the no rule combination for all company_partnership_ids
, partnerships_rules_no_rule_combinations as (

    select 
        '0' as partnership_rule_combinations_id
        , company_partnership_id
        , company_id
        , partnership_id
        , collect_list(partnership_rule_id) as partnership_rule_id_combination_list
        , collect_list(partnership_rule_description) as partnership_rule_combination_list
    from partnerships_rules
    where company_partnership_rule_id = '0'
    group by all

)

, partnership_rule_combinations_unioned as (

    select * from partnership_rule_combinations_distinct

    union all 

    select * from partnerships_rules_no_rule_combinations

)


, pk_added as (

    select
        case 
        when company_partnership_id = '0' 
            then '0'
        else
            md5(
                concat(
                    partnership_rule_combinations_id
                    , company_partnership_id
                    )
            )
        end as pk_dim_partnership_rule_combinations
        , partnership_rule_combinations_id
        , company_partnership_id
        , company_id
        , partnership_id
        , partnership_rule_id_combination_list
        , partnership_rule_combination_list
    from partnership_rule_combinations_unioned

)


select * from pk_added
