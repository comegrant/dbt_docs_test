with loyalty_level_companies as (

    select * from {{ ref('cms__loyalty_level_companies') }}

)

, loyalty_levels as (

    select * from {{ ref('cms__loyalty_levels') }}

)

, joined as (

    select 
        loyalty_level_companies.*
        , loyalty_levels.loyalty_level_number
    
    from loyalty_level_companies
    left join loyalty_levels 
        on loyalty_level_companies.loyalty_level_id = loyalty_levels.loyalty_level_id

)

select * from joined