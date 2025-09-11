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


, company_partnerships_rules_joined as (

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

-- to cover the case where a partnership exists and no rules apply 
, company_partnerships_no_rule as (

    select distinct
        company_partnership_id
        , company_id
        , is_active_partnership
        , partnership_id
        , partnership_name
        , '0' as company_partnership_rule_id
        , true as is_active_rule
        , '0' as partnership_rule_id
        , 'No partnership rule' as partnership_rule_name
        , 'No partnership rule' as partnership_rule_description

    from company_partnerships_rules_joined

)

-- to cover the case where no partnership applies
, company_partnerships_no_partnership as (

    select
        '0' as company_partnership_id
        , '0' as company_id
        , true as is_active_partnership
        , '0' as partnership_id
        , 'No partnership' as partnership_name
        , '0' as company_partnership_rule_id
        , true as is_active_rule
        , '0' as partnership_rule_id
        , 'No partnership' as partnership_rule_name
        , 'No partnership' as partnership_rule_description

)

, all_tables_unioned as (

    select * from company_partnerships_rules_joined
    
    union all
    
    select * from company_partnerships_no_rule

    union all 

    select * from company_partnerships_no_partnership


)

select * from all_tables_unioned