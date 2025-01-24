with 

loyalty_ledger as (

    select * from {{ ref('cms__loyalty_agreement_ledger')}}
    
)

, loyalty_level_companies as (

    select * from {{ ref('cms__loyalty_level_companies')}}
    
)

, loyalty_levels as (

    select * from {{ ref('cms__loyalty_levels')}}
    
)

, loyalty_ledger_and_loyalty_levels_joined as (
    select 
        loyalty_ledger.*,
        loyalty_level_companies.company_id,
        loyalty_level_companies.loyalty_level_name,
        loyalty_levels.loyalty_level_id,
        loyalty_levels.loyalty_level_number
    from loyalty_ledger
    left join loyalty_level_companies
        on loyalty_ledger.loyalty_level_company_id = loyalty_level_companies.loyalty_level_company_id
    left join loyalty_levels
    on loyalty_level_companies.loyalty_level_id = loyalty_levels.loyalty_level_id
)

-- add a counter that restarts every time the loyalty level changes for a billing agreement
, loyalty_levels_group as (
    select
        loyalty_ledger_and_loyalty_levels_joined.*
        , row_number() over (
            partition by billing_agreement_id 
            order by points_generated_at
        ) - 
        row_number() over (
            partition by  billing_agreement_id, loyalty_level_id 
            order by points_generated_at
        ) as group
    from loyalty_ledger_and_loyalty_levels_joined
)

-- extract all rows where the loyalty level has changed
, loyalty_levels_change as (
    select 
        loyalty_levels_group.* 
        , row_number() over (
            partition by billing_agreement_id, group 
            order by points_generated_at
        ) as row_number
    from loyalty_levels_group
)


, loyalty_levels_scd2 as (
    select 
        billing_agreement_id, 
        company_id,
        loyalty_level_company_id,
        loyalty_level_id,
        loyalty_level_number,
        loyalty_level_name,
        points_generated_at as valid_from,
        {{ get_scd_valid_to('points_generated_at', 'billing_agreement_id') }} as valid_to
    from loyalty_levels_change
    where row_number = 1
)

select * from loyalty_levels_scd2