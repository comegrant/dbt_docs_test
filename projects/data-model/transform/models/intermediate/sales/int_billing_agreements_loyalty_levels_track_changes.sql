with 

loyalty_ledger as (

    select * from {{ ref('cms__loyalty_agreement_ledger')}}
    
)

, group_loyalty_levels (
    select
        billing_agreement_id
        , loyalty_level_id
        , points_generated_at
        , row_number() over (partition by billing_agreement_id, loyalty_level_id order by points_generated_at) as level_group 
    from loyalty_ledger
)

, extract_level_movement as (
    select 
        billing_agreement_id
        , loyalty_level_id
        , points_generated_at as valid_from
        , {{ get_scd_valid_to('points_generated_at', 'billing_agreement_id') }} as valid_to
    from group_loyalty_levels
    where level_group = 1
)

select * from extract_level_movement