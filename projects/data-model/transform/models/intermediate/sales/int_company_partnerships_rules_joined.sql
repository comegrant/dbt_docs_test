with 

partnerships as (

    select * from {{ ref('partnership__partnerships') }}

)

, company_partnerships as (

    select * from {{ ref('partnership__company_partnerships') }}

)

, company_partnership_rules as (

    select * from {{ ref('partnership__company_partnership_rules')}}

)

, partnership_rules as (

    select * from {{ ref('partnership__partnership_rules') }}

)


, partnerships_with_rules_joined as (

    select 

        company_partnerships.company_partnership_id
        , company_partnerships.company_id
        , company_partnerships.is_active_partnership
        , partnerships.partnership_id
        , partnerships.partnership_name
        , company_partnership_rules.company_partnership_rule_id
        , company_partnership_rules.is_active_rule
        , partnership_rules.partnership_rule_id
        , partnership_rules.partnership_rule_name
        , partnership_rules.partnership_rule_description
        

    from partnerships

    left join company_partnerships
        on partnerships.partnership_id = company_partnerships.partnership_id

    left join company_partnership_rules
        on company_partnerships.company_partnership_id = company_partnership_rules.company_partnership_id

    left join partnership_rules
        on company_partnership_rules.partnership_rule_id = partnership_rules.partnership_rule_id

)

select * from partnerships_with_rules_joined