with 

billing_agreement_partnerships as (

    select * from {{ ref('partnership__billing_agreement_partnerships') }}

)

, partnerships_rules_joined as (

    select * from {{ ref('int_company_partnerships_rules_joined') }}

)


, billing_agreements_partnerships_rules_jsonified as (

    select 
        billing_agreement_partnerships.billing_agreement_id
        , partnerships_rules_joined.partnership_id
        , partnerships_rules_joined.company_id
        , partnerships_rules_joined.partnership_name
        , partnerships_rules_joined.is_active_partnership
        -- A string representing a json array of objects containg the rule description and whether it is active. The objects are sorted on the rule description.
        , to_json(
            array_sort(
                collect_list(
                    named_struct(
                        'rule', partnerships_rules_joined.partnership_rule_description
                        , 'is_active', partnerships_rules_joined.is_active_rule
                    )
                ),
                (x,y) -> case when x['rule'] < y['rule'] then -1 
                when x['rule'] > y['rule'] then 1 else 0 end
            )
        ) AS partnership_rules

    from billing_agreement_partnerships

    left join partnerships_rules_joined
        on billing_agreement_partnerships.company_partnership_id = partnerships_rules_joined.company_partnership_id

    group by all

)

select * from billing_agreements_partnerships_rules_jsonified