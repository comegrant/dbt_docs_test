with

company_partnership_rules as (

    select * from {{ ref('int_company_partnerships_rules_joined') }}

)


, add_pk as (

    select
        md5(
            concat(
                company_partnership_id
                , partnership_rule_id
                )
         ) as pk_dim_partnerships
        , company_partnership_id
        , partnership_rule_id
        , company_id
        , partnership_id
        , partnership_name
        , partnership_rule_name
        , partnership_rule_description

    from company_partnership_rules

)

, add_not_relevant_partnership as (

    select
        *
    from add_pk

    union all

    select
        '0' as pk_dim_partnerships
        , '0' as company_partnership_id
        , '0' as partnership_rule_id
        , '0' as company_id
        , '0' as partnership_id
        , 'No partnership' as partnership_name
        , 'No partnership rule' as partnership_rule_name
        , 'No partnership rule' as partnership_rule_description

)

select * from add_not_relevant_partnership
