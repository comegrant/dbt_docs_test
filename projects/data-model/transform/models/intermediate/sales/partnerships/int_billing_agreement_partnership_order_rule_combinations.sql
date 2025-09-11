with 

partnership_order_points as (

    select * from {{ ref('partnership__billing_agreement_partnership_loyalty_points') }}

)

, partnership_rules as (

    select * from {{ ref('int_company_partnerships_rules_joined') }}

)

, partnership_order_points_partnership_rules_joined as (

    select 
        partnership_order_points.billing_agreement_order_id
        , partnership_rules.*
    from partnership_order_points 
    left join partnership_rules
        on partnership_order_points.company_partnership_id = partnership_rules.company_partnership_id
        and partnership_order_points.partnership_rule_id = partnership_rules.partnership_rule_id
    order by billing_agreement_order_id, partnership_rule_id

)

-- rule combinations that exist on orders with partnership points
, partnership_orders_rule_combinations as (

    select 
        billing_agreement_order_id
        , company_partnership_id
        , company_id
        , partnership_id
        , collect_list(partnership_rule_id) as partnership_rule_id_combination_list
        , collect_list(partnership_rule_description) as partnership_rule_combination_list
    from partnership_order_points_partnership_rules_joined
    group by all

)

, id_added as (

    select

        billing_agreement_order_id
        , md5(array_join(partnership_rule_id_combination_list, ',')) as partnership_rule_combinations_id
        , company_partnership_id
        , company_id
        , partnership_id
        , partnership_rule_id_combination_list
        , partnership_rule_combination_list

    from partnership_orders_rule_combinations

)
     

select * from id_added