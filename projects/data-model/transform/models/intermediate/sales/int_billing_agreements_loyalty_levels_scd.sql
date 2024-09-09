with 

loyalty_ledger as (

    select * from {{ ref('cms__loyalty_agreement_ledger')}}
    
)

, loyalty_levels as (

    select * from {{ ref('cms__loyalty_levels')}}
    
)

, join_loyalty_ledger_and_loyalty_levels as (
    select 
        loyalty_ledger.billing_agreement_id,
        loyalty_ledger.loyalty_level_id,
        loyalty_levels.loyalty_level_number,
        loyalty_ledger.points_generated_at
    from loyalty_ledger
    left join loyalty_levels
    on loyalty_ledger.loyalty_level_id = loyalty_levels.loyalty_level_id
)


, group_loyalty_ledger_levels as (
    select
        billing_agreement_id, 
        loyalty_level_id,
        loyalty_level_number,
        points_generated_at,
        row_number() over (partition by billing_agreement_id, loyalty_level_id order by points_generated_at) as level_group 
    from join_loyalty_ledger_and_loyalty_levels
)

, scd_loyalty_ledger_levels as (
    select 
        billing_agreement_id, 
        loyalty_level_id,
        loyalty_level_number,
        points_generated_at as valid_from,
        coalesce(
            lead(points_generated_at, 1) over (partition by billing_agreement_id order by points_generated_at),
            cast('9999-01-01' as timestamp)
        ) as valid_to
    from group_loyalty_ledger_levels
    where level_group = 1
)

select * from scd_loyalty_ledger_levels