with

discount_chains_union as (

    select * from {{ ref('cms__discount_chains') }}

    union all

    select 
        distinct discount_parent_id
        , discount_parent_id as discount_id
        , 0 as discount_chain_order
        , is_active_discount_chain
        , source_created_at
    from {{ ref('cms__discount_chains') }}

)

, only_unique_child_discounts as (

    select * from discount_chains_union
    where discount_id not in (
            select discount_id from discount_chains_union
            group by discount_id
            having count (distinct discount_parent_id) > 1
            )
)

select * from only_unique_child_discounts