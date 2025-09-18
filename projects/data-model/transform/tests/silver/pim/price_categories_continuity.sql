
with 

price_categories as (
    select * from {{ ref('pim__price_categories') }}
)

, non_continuous_price_categories (

    select
        current_level.portion_id
        , current_level.company_id
        , current_level.price_category_level_id 
        , current_level.min_ingredient_cost_inc_vat as min_cost
        , previous_level.max_ingredient_cost_inc_vat as max_cost_previous_level
        , current_level.valid_from as valid_from
        , current_level.valid_to as valid_to
        , previous_level.valid_from as valid_from_previous_level
        , previous_level.valid_to as valid_to_previous_level
    from price_categories as current_level
    left join price_categories as previous_level
        -- Pairing all price categories for a specific portion size and company, 
        -- with the level below (previous level)
        on current_level.portion_id = previous_level.portion_id
        and current_level.company_id = previous_level.company_id
        and current_level.price_category_level_id = previous_level.price_category_level_id + 1
        
        -- Only compare when the validity is overlapping
        and current_level.valid_from < previous_level.valid_to
        and previous_level.valid_from < current_level.valid_to
    
    -- Find the rows where the min cost does not match the max cost of the previous level
    where current_level.min_ingredient_cost_inc_vat <> previous_level.max_ingredient_cost_inc_vat

)

select * from non_continuous_price_categories