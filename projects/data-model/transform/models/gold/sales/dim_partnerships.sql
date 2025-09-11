with

company_partnership_rules as (

    select * from {{ ref('int_company_partnerships_rules_joined') }}

)


, pk_added as (

    select
        case 
        when company_partnership_id = '0' 
            then '0'
        else
            md5(
                concat(
                    company_partnership_id
                    , partnership_rule_id
                    )
            )
        end as pk_dim_partnerships
        , company_partnership_id
        , partnership_rule_id
        , company_id
        , partnership_id
        , partnership_name
        , partnership_rule_name
        , partnership_rule_description

    from company_partnership_rules

)

select * from pk_added
